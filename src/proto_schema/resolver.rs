use command_utils::protobuf::resolve::resolve_proto_imports;

pub(super) fn resolve_primary_message(
    proto: &str,
    message_name: &str,
    imports: &[(&str, &str)],
    primary_import: &str,
) -> Result<String, String> {
    let (start, open) = find_top_level_message(proto, message_name)
        .ok_or_else(|| format!("top-level message {message_name} not found"))?;
    let end = find_block_end(proto, open)
        .ok_or_else(|| format!("message {message_name} has an unclosed body"))?;
    let primary = &proto[start..end];

    let mut remainder = String::with_capacity(proto.len());
    remainder.push_str(&proto[..start]);
    remainder.push_str(&proto[end..]);
    let insertion = preamble_end(&remainder, primary_import)?;
    remainder.insert_str(insertion, &format!("\n{primary}\n"));
    resolve_proto_imports(&remainder, imports)
}

pub(super) fn find_top_level_message(proto: &str, message_name: &str) -> Option<(usize, usize)> {
    find_top_level_declaration(proto, "message", message_name)
}

pub(super) fn find_top_level_enum(proto: &str, enum_name: &str) -> Option<(usize, usize)> {
    find_top_level_declaration(proto, "enum", enum_name)
}

fn find_top_level_declaration(
    proto: &str,
    declaration: &str,
    declaration_name: &str,
) -> Option<(usize, usize)> {
    let bytes = proto.as_bytes();
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut index = 0;

    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        if line_comment {
            line_comment = byte != b'\n';
        } else if block_comment {
            if byte == b'*' && next == Some(b'/') {
                block_comment = false;
                index += 1;
            }
        } else if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'/' && next == Some(b'/') {
            line_comment = true;
            index += 1;
        } else if byte == b'/' && next == Some(b'*') {
            block_comment = true;
            index += 1;
        } else if byte == b'"' {
            in_string = true;
        } else if byte == b'{' {
            depth += 1;
        } else if byte == b'}' {
            depth = depth.checked_sub(1)?;
        } else if depth == 0 && proto[index..].starts_with(declaration) {
            let keyword_end = index + declaration.len();
            if !bytes.get(keyword_end).is_some_and(u8::is_ascii_whitespace) {
                index += 1;
                continue;
            }
            let mut cursor = keyword_end;
            while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
                cursor += 1;
            }
            if proto[cursor..].starts_with(declaration_name) {
                cursor += declaration_name.len();
                if bytes
                    .get(cursor)
                    .is_some_and(|value| value.is_ascii_alphanumeric() || *value == b'_')
                {
                    index += 1;
                    continue;
                }
                while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
                    cursor += 1;
                }
                if bytes.get(cursor) == Some(&b'{') {
                    return Some((index, cursor));
                }
            }
        }
        index += 1;
    }
    None
}

fn preamble_end(proto: &str, primary_import: &str) -> Result<usize, String> {
    let import = format!("import \"{primary_import}\";");
    let import_start = proto
        .find(&import)
        .ok_or_else(|| format!("required import {primary_import} not found"))?;
    Ok(proto[import_start..]
        .find('\n')
        .map_or(proto.len(), |offset| import_start + offset + 1))
}

pub(super) fn find_block_end(proto: &str, open: usize) -> Option<usize> {
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let bytes = proto.as_bytes();
    let mut index = open;

    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        if line_comment {
            line_comment = byte != b'\n';
        } else if block_comment {
            if byte == b'*' && next == Some(b'/') {
                block_comment = false;
                index += 1;
            }
        } else if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'/' && next == Some(b'/') {
            line_comment = true;
            index += 1;
        } else if byte == b'/' && next == Some(b'*') {
            block_comment = true;
            index += 1;
        } else if byte == b'"' {
            in_string = true;
        } else if byte == b'{' {
            depth += 1;
        } else if byte == b'}' {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(index + 1);
            }
        }
        index += 1;
    }
    None
}
