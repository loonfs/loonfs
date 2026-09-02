//! Pagination shared by CLI listing commands.

use crate::args::{PaginationArgs, SeqPaginationArgs};
use crate::error::CliError;
use loonfs_api::PagedResponse;
use std::future::Future;
use std::io::{self, Write};

/// Tracks the total result limit and the size of each request.
pub(super) struct PagePlan {
    limit: Option<u32>,
    page_size: Option<u32>,
    follow_to_end: bool,
    emitted: u32,
}

pub(super) enum PagedListing<P> {
    Streamed,
    Collected(P),
}

/// Fetches pages until the plan is satisfied and returns them as one response.
pub(super) async fn collect_pages<P, F, Fut, O>(
    mut plan: PagePlan,
    cursor: Option<P::Cursor>,
    mut fetch: F,
    mut observe: O,
) -> Result<P, CliError>
where
    P: PagedResponse,
    F: FnMut(Option<P::Cursor>, Option<u32>) -> Fut,
    Fut: Future<Output = Result<P, CliError>>,
    O: FnMut(&P),
{
    let mut collected = fetch(cursor, plan.request_size()).await?;
    observe(&collected);
    plan.record(collected.items().len());
    loop {
        let cursor = collected.next_cursor();
        if !plan.should_continue(cursor.is_some()) {
            return Ok(collected);
        }
        let page = fetch(cursor, plan.request_size()).await?;
        observe(&page);
        plan.record(page.items().len());
        collected.absorb(page);
    }
}

/// Writes each page to stdout as JSON lines when `jsonl` is set, and
/// collects the pages otherwise.
pub(super) async fn collect_or_stream_pages<P, F, Fut, O>(
    mut plan: PagePlan,
    cursor: Option<P::Cursor>,
    jsonl: bool,
    mut fetch: F,
    mut observe: O,
) -> Result<PagedListing<P>, CliError>
where
    P: PagedResponse,
    P::Item: serde::Serialize,
    F: FnMut(Option<P::Cursor>, Option<u32>) -> Fut,
    Fut: Future<Output = Result<P, CliError>>,
    O: FnMut(&P),
{
    if !jsonl {
        return collect_pages(plan, cursor, fetch, observe)
            .await
            .map(PagedListing::Collected);
    }
    let stdout = io::stdout();
    let mut stdout = io::BufWriter::with_capacity(64 * 1024, stdout.lock());
    let mut page = fetch(cursor, plan.request_size()).await?;
    observe(&page);
    plan.record(page.items().len());
    loop {
        write_jsonl_page(&mut stdout, page.items()).map_err(CliError::io)?;
        let cursor = page.next_cursor();
        if !plan.should_continue(cursor.is_some()) {
            return Ok(PagedListing::Streamed);
        }
        page = fetch(cursor, plan.request_size()).await?;
        observe(&page);
        plan.record(page.items().len());
    }
}

/// Writes one JSON item per line and flushes the page before the next request.
pub(super) fn write_jsonl_page<T: serde::Serialize>(
    stdout: &mut impl Write,
    items: &[T],
) -> io::Result<()> {
    for item in items {
        serde_json::to_writer(&mut *stdout, item).map_err(io::Error::other)?;
        stdout.write_all(b"\n")?;
    }
    stdout.flush()
}

impl PagePlan {
    pub(super) fn new(args: &PaginationArgs) -> Self {
        Self::from_values(args.limit, args.page_size, args.all, args.jsonl)
    }

    pub(super) fn for_sequence(args: &SeqPaginationArgs) -> Self {
        Self::from_values(args.limit, args.page_size, args.all, args.jsonl)
    }

    fn from_values(limit: Option<u32>, page_size: Option<u32>, all: bool, jsonl: bool) -> Self {
        Self {
            limit,
            page_size,
            follow_to_end: all || jsonl,
            emitted: 0,
        }
    }

    /// Returns the number of items to request on the next page.
    ///
    /// A total limit larger than one page is split across several requests.
    pub(super) fn request_size(&self) -> Option<u32> {
        self.limit.map_or(self.page_size, |limit| {
            let remaining = limit.saturating_sub(self.emitted);
            Some(
                self.page_size
                    .unwrap_or(loonfs_api::DEFAULT_PAGE_LIMIT)
                    .min(remaining),
            )
        })
    }

    pub(super) fn record(&mut self, items: usize) {
        self.emitted = self
            .emitted
            .saturating_add(u32::try_from(items).unwrap_or(u32::MAX));
    }

    pub(super) fn should_continue(&self, has_next: bool) -> bool {
        has_next
            && self
                .limit
                .map_or(self.follow_to_end, |limit| self.emitted < limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(limit: Option<u32>, page_size: Option<u32>, all: bool) -> PaginationArgs {
        PaginationArgs {
            cursor: None,
            limit,
            page_size,
            all,
            jsonl: false,
        }
    }

    #[test]
    fn a_total_smaller_than_a_page_is_the_first_request_size() {
        let plan = PagePlan::new(&args(Some(3), Some(10), false));
        assert_eq!(plan.request_size(), Some(3));
    }

    #[test]
    fn a_total_larger_than_a_page_follows_until_the_remaining_final_page() {
        let mut plan = PagePlan::new(&args(Some(7), Some(3), false));
        assert_eq!(plan.request_size(), Some(3));
        plan.record(3);
        assert!(plan.should_continue(true));
        assert_eq!(plan.request_size(), Some(3));
        plan.record(3);
        assert_eq!(plan.request_size(), Some(1));
        plan.record(1);
        assert!(!plan.should_continue(true));
    }

    #[test]
    fn one_page_is_the_default_but_all_follows() {
        let mut one_page = PagePlan::new(&args(None, Some(2), false));
        one_page.record(2);
        assert!(!one_page.should_continue(true));

        let mut all = PagePlan::new(&args(None, Some(2), true));
        all.record(2);
        assert!(all.should_continue(true));
        assert!(!all.should_continue(false));

        let mut bounded_all = PagePlan::new(&args(Some(2), Some(2), true));
        bounded_all.record(2);
        assert!(!bounded_all.should_continue(true));
    }
}
