//! The source of the random bytes that generated IDs draw, and the process-wide hook that
//! replaces it.

use std::sync::{Arc, LazyLock, Mutex, RwLock};

/// A source of the random bytes that generated IDs draw.
///
/// [`OsEntropy`] is the default. A host that needs repeatable runs installs another source with
/// [`install_entropy`].
pub trait Entropy: std::fmt::Debug + Send + Sync {
    /// Fills `bytes` with random bytes.
    fn fill(&self, bytes: &mut [u8]);
}

/// The operating system's random generator, the source that generated IDs draw from unless a
/// host installs another one.
#[derive(Debug, Default, Clone, Copy)]
pub struct OsEntropy;

impl Entropy for OsEntropy {
    #[expect(
        clippy::disallowed_methods,
        reason = "the default entropy source reads the system random generator"
    )]
    fn fill(&self, bytes: &mut [u8]) {
        getrandom::fill(bytes).expect("the system random generator should be available");
    }
}

/// A deterministic generator that makes the generated IDs of a run repeatable.
///
/// It isn't a cryptographic generator, because anyone who sees a few of its outputs can predict
/// the rest. It exists so that a host, such as a deterministic simulator, can install it with
/// [`install_entropy`] and replay a run with the same IDs. Two generators made from the same seed
/// produce the same bytes, so a run repeats when it draws its IDs in the same order. A seed's IDs
/// repeat in every run, so each seeded run needs its own store. The generator is `xoshiro256**`,
/// and `splitmix64` expands the seed into its state.
#[derive(Debug)]
pub struct SeededEntropy {
    state: Mutex<[u64; 4]>,
}

impl SeededEntropy {
    /// Creates a generator whose output is determined by `seed`.
    pub fn from_seed(seed: u64) -> Self {
        let mut counter = seed;
        // splitmix64 maps each counter value to a different output, so at most one of the four
        // words is zero, and xoshiro256** needs a state that isn't all zero.
        let state = std::array::from_fn(|_| splitmix64(&mut counter));
        Self {
            state: Mutex::new(state),
        }
    }
}

impl Entropy for SeededEntropy {
    fn fill(&self, bytes: &mut [u8]) {
        let mut state = self
            .state
            .lock()
            .expect("the seeded entropy lock should not be poisoned");
        for chunk in bytes.chunks_mut(8) {
            let word = xoshiro256_star_star(&mut state).to_be_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }
}

/// Advances a `splitmix64` counter and returns its next output.
fn splitmix64(counter: &mut u64) -> u64 {
    *counter = counter.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut mixed = *counter;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

/// Advances a `xoshiro256**` state and returns its next output.
fn xoshiro256_star_star(state: &mut [u64; 4]) -> u64 {
    let output = state[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
    let shifted = state[1] << 17;
    state[2] ^= state[0];
    state[3] ^= state[1];
    state[1] ^= state[2];
    state[0] ^= state[3];
    state[2] ^= shifted;
    state[3] = state[3].rotate_left(45);
    output
}

/// The source that every generated ID in the process draws from.
static INSTALLED_ENTROPY: LazyLock<RwLock<Arc<dyn Entropy>>> =
    LazyLock::new(|| RwLock::new(Arc::new(OsEntropy)));

/// Replaces the entropy source that every generated ID in the process draws from, and returns
/// the previous source.
///
/// The source is process-wide rather than a runtime setting, because value constructors such as
/// [`ContentId::generate`](crate::ContentId::generate) make IDs without a runtime. Installing a
/// source changes the IDs that every runtime in the process generates. A production host never
/// installs a source, so its IDs come from [`OsEntropy`]. A host that installs a
/// [`SeededEntropy`] for a repeatable run can install the returned source again when the run
/// ends.
pub fn install_entropy(source: Arc<dyn Entropy>) -> Arc<dyn Entropy> {
    let mut installed = INSTALLED_ENTROPY
        .write()
        .expect("the installed entropy lock should not be poisoned");
    std::mem::replace(&mut *installed, source)
}

/// Returns the entropy source that generated IDs draw from.
pub fn installed_entropy() -> Arc<dyn Entropy> {
    let installed = INSTALLED_ENTROPY
        .read()
        .expect("the installed entropy lock should not be poisoned");
    Arc::clone(&installed)
}

#[cfg(test)]
mod tests {
    use super::{splitmix64, xoshiro256_star_star};

    #[test]
    fn the_seeded_generator_matches_the_published_splitmix64_and_xoshiro256_star_star_outputs() {
        let mut counter = 0;
        assert_eq!(
            [(); 4].map(|()| splitmix64(&mut counter)),
            [
                0xe220_a839_7b1d_cdaf,
                0x6e78_9e6a_a1b9_65f4,
                0x06c4_5d18_8009_454f,
                0xf88b_b8a8_724c_81ec,
            ]
        );
        let mut state = [1, 2, 3, 4];
        assert_eq!(
            [(); 6].map(|()| xoshiro256_star_star(&mut state)),
            [
                11_520,
                0,
                1_509_978_240,
                1_215_971_899_390_074_240,
                1_216_172_134_540_287_360,
                607_988_272_756_665_600,
            ]
        );
    }
}
