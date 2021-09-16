use std::io::{Read, Write};
use std::iter::repeat;
use std::process::{Command, Stdio};

fn run_script<T: AsRef<str>>(commands: &[T]) -> Vec<String> {
    let mut child = Command::new("cargo")
        .arg("run")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn child process");

    let mut stdin = child.stdin.take().expect("Failed to open stdin");
    let stdout = &mut child.stdout.take().expect("Failed to open stdout");
    let cmds = commands.iter();
    let mut outputs = vec![];
    cmds.for_each(|cmd| {
        stdin
            .write_all(&[cmd.as_ref().as_bytes(), b"\n"].concat())
            .expect("Failed to write to stdin");

        let end_of_output = b"\ndb > ";
        let mut seen = vec![];
        let mut output: Vec<u8> = stdout
            .bytes()
            .filter_map(|b| b.ok())
            .take_while(|b| {
                if seen.len() < end_of_output.len() {
                    seen.push(*b)
                } else {
                    seen.remove(0);
                    seen.push(*b);
                }

                &seen != end_of_output
            })
            .collect();

        match seen.last() {
            Some(c) => output.push(*c),
            _ => (),
        }

        let buf = String::from_utf8_lossy(&output).to_string();
        if buf != "" {
            outputs.push(buf);
        }
    });

    let _ = child.wait().expect("Failed to read stdout");
    outputs
}

#[test]
fn database_inserts_and_retrieves_a_row() {
    let output = run_script(&["insert 1 user1 person1@example.com", "select", ".exit"]);
    assert_eq!(
        output,
        vec![
            vec![
                "db > processing statement \"insert 1 user1 person1@example.com\"",
                "executing insert statement",
                "result Success",
                "db > ",
            ]
            .join("\n"),
            vec![
                "processing statement \"select\"",
                "executing select statement",
                "1, \"user1\", \"person1@example.com\"",
                "db > "
            ]
            .join("\n")
        ]
    );
}

#[test]
fn prints_error_message_when_table_is_full() {
    let insert_cmds: Vec<String> = (1..1402)
        .map(|i| format!("insert {} user{} person{}@example.com", i, i, i))
        .collect();

    let cmds = [insert_cmds, vec![".exit".into()]].concat();
    let output = run_script(&cmds);
    assert_eq!(
        *output.last().unwrap(),
        vec![
            "processing statement \"insert 1401 user1401 person1401@example.com\"",
            "executing insert statement",
            "db message: Execute(TableFull)",
            "db > "
        ]
        .join("\n")
    );
}

#[test]
fn allows_inserting_and_selecting_strings_that_are_the_max_length() {
    let long_username: String = repeat("a").take(32).collect();
    let long_email: String = repeat("a").take(255).collect();

    let cmds = [
        format!("insert 1 {} {}", long_username, long_email),
        "select".into(),
        ".exit".into(),
    ];
    let output = run_script(&cmds);
    assert_eq!(
        output,
        vec![
            vec![
                "db > processing statement \"insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"", 
                "executing insert statement",
                "result Success",
                "db > "
            ].join("\n"), 
            vec![
                "processing statement \"select\"",
                "executing select statement",
                "1, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"",
                "db > "
            ].join("\n")
        ]
    );
}

#[test]
fn prints_error_messages_if_strings_are_too_long() {
    let long_username: String = repeat("a").take(33).collect();
    let long_email: String = repeat("a").take(256).collect();

    let cmds = [
        format!("insert 1 {} {}", long_username, long_email),
        ".exit".into(),
    ];
    let output = run_script(&cmds);
    assert_eq!(
        output,
        vec![
            vec![
                "db > processing statement \"insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"",
                "db message: Statement(TooLong)",
                "db > "
            ].join("\n")
        ]
    );
}

#[test]
fn prints_error_messages_if_id_is_negative() {
    let long_username = "a";
    let long_email = "a";

    let cmds = [
        format!("insert -1 {} {}", long_username, long_email),
        ".exit".into(),
    ];
    let output = run_script(&cmds);
    assert_eq!(
        output,
        vec![vec![
            "db > processing statement \"insert -1 a a\"",
            "db message: Statement(InvalidId)",
            "db > "
        ]
        .join("\n")]
    );
}
