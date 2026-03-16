mod unit;

#[cfg(test)]
use std::sync::Once;

#[cfg(test)]
use log::LevelFilter;

#[cfg(test)]
static INIT: Once = Once::new();

#[cfg(test)]
fn init() {
    INIT.call_once(|| {
        env_logger::builder()
            .is_test(true)
            .filter_level(LevelFilter::Trace)
            .init()
    })
}
