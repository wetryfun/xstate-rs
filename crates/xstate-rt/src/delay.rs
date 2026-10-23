//! Duration strings, as XState's `parseDurationToMilliseconds`: `500ms`,
//! `1.5s`, and ISO 8601 durations such as `PT1M30S` or `P1DT2H`.

pub(crate) fn parse_duration_ms(input: &str) -> Option<f64> {
    let s = input.trim();
    if let Some(ms) = strip_suffix_ci(s, "ms") {
        if !ms.is_empty() && ms.bytes().all(|b| b.is_ascii_digit()) {
            return ms.parse::<f64>().ok();
        }
    }
    if let Some(secs) = strip_suffix_ci(s, "s") {
        if let Some(ms) = parse_seconds(secs) {
            return Some(ms);
        }
    }
    parse_iso8601(s)
}

fn strip_suffix_ci<'a>(s: &'a str, suffix: &str) -> Option<&'a str> {
    if s.len() >= suffix.len() && s[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix) {
        Some(&s[..s.len() - suffix.len()])
    } else {
        None
    }
}

/// `(\d*)(\.?)(\d*)s`, keeping at most three fractional digits.
fn parse_seconds(s: &str) -> Option<f64> {
    let (whole, frac, has_dot) = match s.split_once('.') {
        Some((w, f)) => (w, f, true),
        None => (s, "", false),
    };
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let whole_ms = if whole.is_empty() { 0.0 } else { whole.parse::<f64>().ok()? * 1000.0 };
    let frac_ms = if has_dot && !frac.is_empty() {
        let mut digits: String = frac.chars().take(3).collect();
        while digits.len() < 3 {
            digits.push('0');
        }
        digits.parse::<f64>().ok()?
    } else {
        0.0
    };
    Some(whole_ms + frac_ms)
}

fn parse_iso8601(s: &str) -> Option<f64> {
    let rest = s.strip_prefix('P').or_else(|| s.strip_prefix('p'))?;
    let (date, time) = match rest.find(['T', 't']) {
        Some(i) => (&rest[..i], Some(&rest[i + 1..])),
        None => (rest, None),
    };
    let mut total = 0.0;
    let mut any = false;
    let mut parse_part = |part: &str, units: &[(char, f64)]| -> Option<()> {
        let mut remaining = part;
        let mut unit_index = 0;
        while !remaining.is_empty() {
            let end = remaining.find(|c: char| c.is_ascii_alphabetic())?;
            let number = &remaining[..end];
            let unit = remaining[end..].chars().next()?.to_ascii_uppercase();
            let pos = units[unit_index..].iter().position(|(u, _)| *u == unit)? + unit_index;
            if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit() || b == b'.' || b == b',') {
                return None;
            }
            total += number.replace(',', ".").parse::<f64>().ok()? * units[pos].1;
            any = true;
            unit_index = pos + 1;
            remaining = &remaining[end + 1..];
        }
        Some(())
    };
    const DAY: f64 = 24.0 * 60.0 * 60.0 * 1000.0;
    parse_part(date, &[('W', 7.0 * DAY), ('D', DAY)])?;
    if let Some(time) = time {
        parse_part(time, &[('H', 3_600_000.0), ('M', 60_000.0), ('S', 1000.0)])?;
    }
    any.then_some(total)
}

/// Converts a millisecond value to the `u64` the scheduler uses. Negative and
/// non-finite values are rejected; fractions are rounded up so a timer never
/// fires early.
pub(crate) fn ms_to_u64(ms: f64) -> Option<u64> {
    if ms.is_finite() && ms >= 0.0 {
        Some(ms.ceil() as u64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::parse_duration_ms as p;

    #[test]
    fn durations() {
        assert_eq!(p("100ms"), Some(100.0));
        assert_eq!(p("5s"), Some(5000.0));
        assert_eq!(p("1.5s"), Some(1500.0));
        assert_eq!(p(".25s"), Some(250.0));
        assert_eq!(p("PT1M30S"), Some(90_000.0));
        assert_eq!(p("P1D"), Some(86_400_000.0));
        assert_eq!(p("PT0.5S"), Some(500.0));
        assert_eq!(p("P"), None);
        assert_eq!(p("1m"), None);
        assert_eq!(p("2h"), None);
        assert_eq!(p("abc"), None);
        assert_eq!(p("PT1S1M"), None);
    }
}
