use anyhow::{bail, Result};
use loon_testkit::{render::render_summary, scenario::Scenario};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("render-case") => {
            let path = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing scenario path"))?;
            let scenario = Scenario::load(path)?;
            println!("{}", render_summary(&scenario));
            Ok(())
        }
        Some("replay-seed") => {
            println!("TODO: wire deterministic replay");
            Ok(())
        }
        Some("minimize-case") => {
            println!("TODO: wire scenario minimization");
            Ok(())
        }
        Some(other) => bail!("unknown xtask command: {other}"),
        None => {
            println!("xtask commands: render-case | replay-seed | minimize-case");
            Ok(())
        }
    }
}
