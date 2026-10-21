//! Pagination shared by CLI listing commands.

use crate::args::PageLimitArgs;
use crate::error::CliError;
use loonfs_types::{usize_from_u64, LimitError, PagedResponse, Pager, PaginationPolicy};

pub(super) enum PagedListing<P> {
    Streamed,
    Collected(P),
}

/// Visits and releases each page before loading the next one.
///
/// `--page-size` and `--limit` size the pager's requests. The listing
/// follows every page with `--all`, `--jsonl`, or `--limit`, and stops after
/// the first page otherwise.
pub(super) async fn visit_pages<P, E, V>(
    mut pager: Pager<P, E>,
    args: &PageLimitArgs,
    mut visit: V,
) -> Result<(), CliError>
where
    P: PagedResponse,
    CliError: From<E>,
    V: FnMut(P) -> Result<(), CliError>,
{
    // Both flags meet the pagination contract here, with the error the
    // server gives for an invalid `limit`.
    if let Some(page_size) = args.page_size {
        let page_limit = PaginationPolicy::default()
            .effective_limit(Some(page_size))
            .map_err(invalid_limit)?;
        pager = pager.page_limit(page_limit);
    }
    if let Some(limit) = args.limit {
        if limit == 0 {
            return Err(invalid_limit(LimitError::Zero));
        }
        pager = pager.max_items(usize_from_u64(u64::from(limit)));
    }
    let follows = args.all || args.jsonl || args.limit.is_some();
    while let Some(page) = pager.next().await {
        visit(page?)?;
        if !follows {
            break;
        }
    }
    Ok(())
}

/// Writes each page to stdout as JSON lines as it arrives when `--jsonl` is
/// set, and collects the pages into one response otherwise.
pub(super) async fn collect_or_stream_pages<P, E, O>(
    pager: Pager<P, E>,
    args: &PageLimitArgs,
    mut observe: O,
) -> Result<PagedListing<P>, CliError>
where
    P: PagedResponse,
    P::Item: serde::Serialize,
    CliError: From<E>,
    O: FnMut(&P),
{
    let mut collected: Option<P> = None;
    visit_pages(pager, args, |page| {
        observe(&page);
        if args.jsonl {
            return crate::render::write_jsonl_page(page.items()).map_err(CliError::io);
        }
        match &mut collected {
            Some(collected) => collected.absorb(page),
            None => collected = Some(page),
        }
        Ok(())
    })
    .await?;
    if args.jsonl {
        return Ok(PagedListing::Streamed);
    }
    Ok(PagedListing::Collected(collected.expect(
        "a listing with a nonzero limit should load its first page",
    )))
}

fn invalid_limit(error: LimitError) -> CliError {
    CliError::invalid_request(error.to_string()).with_param("limit")
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_types::{ErrorCode, PageRequest, DEFAULT_MAX_PAGE_LIMIT};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct NumberPage {
        numbers: Vec<u32>,
        next_cursor: Option<u32>,
    }

    impl PagedResponse for NumberPage {
        type Item = u32;
        type Cursor = u32;

        fn items_mut(&mut self) -> &mut Vec<u32> {
            &mut self.numbers
        }

        fn items(&self) -> &[u32] {
            &self.numbers
        }

        fn next_cursor(&self) -> Option<u32> {
            self.next_cursor
        }

        fn absorb(&mut self, mut later: Self) {
            self.numbers.append(&mut later.numbers);
            self.next_cursor = later.next_cursor;
        }
    }

    /// Serves the numbers below `count` and records the size of each request.
    fn numbers(count: u32, requests: Arc<Mutex<Vec<u32>>>) -> Pager<NumberPage, CliError> {
        Pager::new(move |request: PageRequest<u32>| {
            requests
                .lock()
                .expect("request log lock")
                .push(request.limit.get());
            let start = request.cursor.map_or(0, |cursor| cursor + 1);
            let numbers: Vec<u32> = (start..count).take(request.limit.as_usize()).collect();
            let next_cursor = numbers.last().copied().filter(|last| last + 1 < count);
            std::future::ready(Ok(NumberPage {
                numbers,
                next_cursor,
            }))
        })
    }

    fn args(limit: Option<u32>, page_size: Option<u32>, all: bool, jsonl: bool) -> PageLimitArgs {
        PageLimitArgs {
            limit,
            page_size,
            all,
            jsonl,
        }
    }

    #[tokio::test]
    async fn pages_are_delivered_before_the_next_fetch_even_if_it_fails() {
        let delivered = Arc::new(AtomicBool::new(false));
        let pager = Pager::new({
            let delivered = delivered.clone();
            move |request: PageRequest<u32>| {
                let page = match request.cursor {
                    Some(_) => {
                        assert!(
                            delivered.load(Ordering::SeqCst),
                            "the first page must already be delivered"
                        );
                        Err(CliError::invalid_request("second page failed"))
                    }
                    None => Ok(NumberPage {
                        numbers: Vec::new(),
                        next_cursor: Some(0),
                    }),
                };
                std::future::ready(page)
            }
        });
        let result = visit_pages(pager, &args(None, Some(1), true, false), |_| {
            delivered.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;
        result.expect_err("the failed second fetch should fail the listing");
        assert!(delivered.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn page_size_and_limit_size_each_request_through_the_pager() {
        // Each case is the listing's length, the flags, and the size of
        // each request.
        let cases = [
            (20, args(Some(3), Some(10), false, false), vec![3]),
            (20, args(Some(7), Some(3), false, false), vec![3, 3, 1]),
            (20, args(None, Some(2), false, false), vec![2]),
            (5, args(None, Some(2), true, false), vec![2, 2, 2]),
            (5, args(None, Some(2), false, true), vec![2, 2, 2]),
            (20, args(Some(2), Some(2), true, false), vec![2]),
            (2_000, args(Some(1_001), None, false, false), vec![1_000, 1]),
            (2_000, args(None, None, false, false), vec![1_000]),
        ];
        for (count, args, expected) in cases {
            let requests = Arc::new(Mutex::new(Vec::new()));
            visit_pages(numbers(count, requests.clone()), &args, |_| Ok(()))
                .await
                .expect("a listing within the pagination contract");
            assert_eq!(
                *requests.lock().expect("request log lock"),
                expected,
                "{args:?}"
            );
        }

        for invalid in [
            args(Some(0), None, false, false),
            args(None, Some(0), false, false),
            args(None, Some(DEFAULT_MAX_PAGE_LIMIT + 1), false, false),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let error = visit_pages(numbers(20, requests.clone()), &invalid, |_| Ok(()))
                .await
                .expect_err("a flag outside the pagination contract");
            assert_eq!(
                error.code,
                ErrorCode::InvalidRequest.as_str(),
                "{invalid:?}"
            );
            assert!(requests.lock().expect("request log lock").is_empty());
        }
    }
}
