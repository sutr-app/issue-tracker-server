use command_utils::protobuf::ProtobufDescriptor;

pub(super) fn descriptor(proto: &str) -> ProtobufDescriptor {
    ProtobufDescriptor::new(&proto.to_owned()).expect("published proto must compile")
}

pub(super) fn assert_primary_message(proto: &str, expected: &str) {
    assert!(
        !proto
            .lines()
            .any(|line| line.trim_start().starts_with("import ")),
        "published proto must be self-contained"
    );
    assert_eq!(
        descriptor(proto)
            .get_messages()
            .first()
            .expect("published proto must contain a message")
            .name(),
        expected
    );
}

pub(super) fn assert_dense_proto_numbering(proto_name: &str, proto: &str) {
    assert!(
        !proto.contains("reserved"),
        "{proto_name} must not retain pre-release reserved declarations"
    );

    let tokens = tokenize_proto(proto);
    assert_dense_declarations(proto_name, &tokens);
}

fn tokenize_proto(proto: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = proto.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '/' if chars.peek() == Some(&'/') => {
                chars.next();
                while chars.next().is_some_and(|next| next != '\n') {}
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                while let Some(next) = chars.next() {
                    if next == '*' && chars.peek() == Some(&'/') {
                        chars.next();
                        break;
                    }
                }
            }
            '"' => {
                while let Some(next) = chars.next() {
                    if next == '\\' {
                        chars.next();
                    } else if next == '"' {
                        break;
                    }
                }
            }
            '{' | '}' | '=' | ';' => tokens.push(character.to_string()),
            character
                if character.is_ascii_alphanumeric() || character == '_' || character == '.' =>
            {
                let mut token = character.to_string();
                while chars.peek().is_some_and(|next| {
                    next.is_ascii_alphanumeric() || *next == '_' || *next == '.'
                }) {
                    token.push(chars.next().expect("peeked character must exist"));
                }
                tokens.push(token);
            }
            _ => {}
        }
    }
    tokens
}

fn assert_dense_declarations(proto_name: &str, tokens: &[String]) {
    let mut index = 0;
    while index < tokens.len() {
        match tokens[index].as_str() {
            "message" | "enum" if tokens.get(index + 2).is_some_and(|token| token == "{") => {
                let declaration_kind = tokens[index].as_str();
                let declaration_name = tokens.get(index + 1).expect("declaration must have a name");
                let body_start = find_open_brace(tokens, index + 2);
                let body_end = find_matching_brace(tokens, body_start);
                let tags = if declaration_kind == "message" {
                    message_field_tags(proto_name, tokens, body_start + 1, body_end)
                } else {
                    enum_value_tags(tokens, body_start + 1, body_end)
                };
                let expected: Vec<u32> = if declaration_kind == "message" {
                    (1..=tags.len() as u32).collect()
                } else {
                    (0..tags.len() as u32).collect()
                };
                assert_eq!(
                    tags, expected,
                    "{proto_name} {declaration_kind} {declaration_name} must use contiguous tags"
                );
                assert_dense_declarations(proto_name, &tokens[body_start + 1..body_end]);
                index = body_end + 1;
            }
            _ => index += 1,
        }
    }
}

fn message_field_tags(proto_name: &str, tokens: &[String], start: usize, end: usize) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut index = start;
    while index < end {
        match tokens[index].as_str() {
            "message" | "enum" if tokens.get(index + 2).is_some_and(|token| token == "{") => {
                index = find_matching_brace(tokens, find_open_brace(tokens, index + 2)) + 1;
            }
            "oneof" => {
                let body_start = find_open_brace(tokens, index + 2);
                let body_end = find_matching_brace(tokens, body_start);
                tags.extend(message_field_tags(
                    proto_name,
                    tokens,
                    body_start + 1,
                    body_end,
                ));
                index = body_end + 1;
            }
            _ => {
                let statement_end = tokens[index..end]
                    .iter()
                    .position(|token| token == ";")
                    .map_or(end, |offset| index + offset);
                if let Some(equals) = tokens[index..statement_end]
                    .iter()
                    .position(|token| token == "=")
                {
                    let tag = tokens[index + equals + 1]
                        .parse()
                        .unwrap_or_else(|_| panic!("{proto_name} field tag must be numeric"));
                    tags.push(tag);
                }
                index = statement_end + 1;
            }
        }
    }
    tags.sort_unstable();
    tags
}

fn enum_value_tags(tokens: &[String], start: usize, end: usize) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut index = start;
    while index < end {
        let statement_end = tokens[index..end]
            .iter()
            .position(|token| token == ";")
            .map_or(end, |offset| index + offset);
        if let Some(equals) = tokens[index..statement_end]
            .iter()
            .position(|token| token == "=")
        {
            tags.push(
                tokens[index + equals + 1]
                    .parse()
                    .expect("enum value must be numeric"),
            );
        }
        index = statement_end + 1;
    }
    tags.sort_unstable();
    tags
}

fn find_open_brace(tokens: &[String], start: usize) -> usize {
    start
        + tokens[start..]
            .iter()
            .position(|token| token == "{")
            .expect("declaration must have a body")
}

fn find_matching_brace(tokens: &[String], start: usize) -> usize {
    let mut depth = 0;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        match token.as_str() {
            "{" => depth += 1,
            "}" => {
                depth -= 1;
                if depth == 0 {
                    return index;
                }
            }
            _ => {}
        }
    }
    panic!("declaration body must close");
}
