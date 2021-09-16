use std::io::{Read, Write};
use std::iter::repeat;
use std::process::{Command, Stdio};

fn run_script(commands: &[&str]) -> Vec<String> {
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
            .write_all(&[cmd.as_bytes(), b"\n"].concat())
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

        println!("read {} bytes", output.len());

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
    let output = run_script(&["insert 1 g g", "select", ".exit"]);
    assert_eq!(
        output.join(""),
        vec![
            "db > processing statement \"insert 1 g g\"",
            "executing insert statement",
            "result Success",
            "db > processing statement \"select\"",
            "executing select statement",
            "1, \"g\", \"g\"",
            "db > "
        ]
        .join("\n")
    );
}

#[test]
fn prints_error_message_when_table_is_full() {
    let insert_cmds: Vec<String> = (1..1402)
        .map(|i| format!("insert {} user{} person{}@example.com", i, i, i))
        .collect();

    println!("inserting {:?} rows", insert_cmds.len());

    let cmds = [insert_cmds, vec![".exit".into()]].concat();
    let cmds: Vec<&str> = cmds.iter().map(|s| s.as_str()).collect();
    let output = run_script(&cmds);
    assert_eq!(
        output[output.len()-1..],
        vec![
            "processing statement \"insert 1401 user1401 person1401@example.com\"\nexecuting insert statement\ndb message: Execute(TableFull)\ndb > "
        ]
    );
}

#[test]
fn allows_inserting_strings_that_are_the_max_length() {
    let long_username: String = repeat("a").take(32).collect();
    let long_email: String = repeat("a").take(255).collect();

    let cmds = [
        format!("insert 1 {} {}", long_username, long_email),
        "select".into(),
        ".exit".into(),
    ];
    let cmds: Vec<&str> = cmds.iter().map(|s| s.as_str()).collect();
    let output = run_script(&cmds);
    assert_eq!(
        output[output.len()-1..],
        vec![
            "processing statement \"select\"\nexecuting select statement\n1, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\ndb > "
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
    let cmds: Vec<&str> = cmds.iter().map(|s| s.as_str()).collect();
    let output = run_script(&cmds);
    assert_eq!(
        output[output.len() - 1..],
        vec![
            "db > processing statement \"insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\ndb message: Statement(TooLong)\ndb > "
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
    let cmds: Vec<&str> = cmds.iter().map(|s| s.as_str()).collect();
    let output = run_script(&cmds);
    assert_eq!(
        output[output.len() - 1..],
        vec![
            "db > processing statement \"insert -1 a a\"\ndb message: Statement(InvalidId)\ndb > "
        ]
    );
}
