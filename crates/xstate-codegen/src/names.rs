//! Rust identifiers from JSON names.

const KEYWORDS: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "dyn", "else", "enum", "extern", "false", "fn", "for", "if",
    "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "static", "struct", "trait",
    "true", "type", "unsafe", "use", "where", "while", "abstract", "become", "box", "do", "final", "gen", "macro",
    "override", "priv", "try", "typeof", "unsized", "virtual", "yield",
];

/// Splits `fooBar`, `foo_bar`, `foo-bar`, `foo.bar` and `FOOBar` into words.
fn words(s: &str) -> Vec<String> {
    let chars: Vec<char> = s.chars().collect();
    let mut words = Vec::new();
    let mut current = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if !c.is_ascii_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            continue;
        }
        if c.is_ascii_uppercase() && !current.is_empty() {
            let prev = chars[i - 1];
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_ascii_lowercase());
            if prev.is_ascii_lowercase() || prev.is_ascii_digit() || (prev.is_ascii_uppercase() && next_lower) {
                words.push(std::mem::take(&mut current));
            }
        }
        current.push(c);
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// `PascalCase`, for types and variants.
pub fn pascal(s: &str) -> String {
    let mut out: String = words(s)
        .iter()
        .map(|w| {
            let mut cs = w.chars();
            let first = cs.next().unwrap().to_ascii_uppercase();
            std::iter::once(first).chain(cs.map(|c| c.to_ascii_lowercase())).collect::<String>()
        })
        .collect();
    if out.is_empty() {
        out.push_str("Empty");
    }
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, 'N');
    }
    out
}

/// `snake_case`, for fields and methods. Keywords become raw identifiers.
pub fn snake(s: &str) -> String {
    let mut out = words(s).iter().map(|w| w.to_ascii_lowercase()).collect::<Vec<_>>().join("_");
    if out.is_empty() {
        out.push_str("empty");
    }
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    match out.as_str() {
        "self" | "super" | "crate" | "_" => out.push('_'),
        k if KEYWORDS.contains(&k) => out.insert_str(0, "r#"),
        _ => {}
    }
    out
}

/// The identifier without a raw prefix: the name serde sees.
pub fn unraw(ident: &str) -> &str {
    ident.strip_prefix("r#").unwrap_or(ident)
}

/// A Rust string literal.
pub fn string_lit(s: &str) -> String {
    format!("{s:?}")
}

/// A raw string literal that can hold `s`.
pub fn raw_lit(s: &str) -> String {
    let mut hashes = 1;
    while s.contains(&format!("\"{}", "#".repeat(hashes))) {
        hashes += 1;
    }
    let h = "#".repeat(hashes);
    format!("r{h}\"{s}\"{h}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cases() {
        assert_eq!(pascal("user.login"), "UserLogin");
        assert_eq!(pascal("hasAccess"), "HasAccess");
        assert_eq!(pascal("HTTPRequest"), "HttpRequest");
        assert_eq!(pascal("2fa"), "N2fa");
        assert_eq!(pascal(""), "Empty");
        assert_eq!(snake("hasAccess"), "has_access");
        assert_eq!(snake("cart.add"), "cart_add");
        assert_eq!(snake("type"), "r#type");
        assert_eq!(snake("self"), "self_");
        assert_eq!(snake("last4"), "last4");
        assert_eq!(snake("APIKey"), "api_key");
        assert_eq!(raw_lit("a\"#b"), "r##\"a\"#b\"##");
    }
}
