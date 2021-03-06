use std::io::{ErrorKind, Write};
use std::iter::repeat;
use std::path::Path;
use std::process::{Command, Stdio};

fn run_script(commands: Vec<String>, test_file_name: &str) -> Vec<String> {
    let mut child = Command::new("cargo")
        .arg("run")
        .arg(test_file_name)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn child process");

    let mut stdin = child.stdin.take().expect("failed to get stdin");

    // If the child process fills its stdout buffer, it may end up
    // waiting until the parent reads the stdout, and not be able to
    // read stdin in the meantime, causing a deadlock.
    // Writing from another thread ensures that stdout is being read
    // at the same time, avoiding the problem.
    let handle = std::thread::spawn(move || {
        commands.iter().for_each(|cmd| {
            stdin
                .write_all(&[cmd.as_bytes(), b"\n"].concat())
                .expect("Failed to write to stdin");
        });
    });

    // wait for output also attempts to read from the buffer for stdout which stops us from hanging
    let output = child.wait_with_output().expect("Failed to read stdout");
    handle.join().unwrap();
    let stringified = String::from_utf8_lossy(&output.stdout).to_string();
    stringified.split("\n").map(String::from).collect()
}

fn ensure_clean_fs<P>(test_file_name: P)
where
    P: AsRef<Path>,
{
    std::fs::remove_file(test_file_name)
        .or_else(|e| match e.kind() {
            ErrorKind::NotFound => Ok(()),
            _ => Err(e),
        })
        .expect("could not clean up database files before running tests");
}

fn clean_test(test_case: &str, test: fn(&str)) -> impl Fn() {
    let test_file_name = format!("test-database-for-{}.db", test_case);
    let clean_test_wrapper = move || {
        ensure_clean_fs(&test_file_name);
        test(&test_file_name);
        ensure_clean_fs(&test_file_name);
    };
    clean_test_wrapper
}

#[test]
fn database_inserts_and_retrieves_a_row() {
    let test_case = "database_inserts_and_retrieves_a_row";

    let test = |test_file_name: &str| {
        let output = run_script(
            vec![
                "insert 1 user1 person1@example.com".into(),
                "select".into(),
                ".exit".into(),
            ],
            &test_file_name,
        );
        assert_eq!(
            output,
            vec![
                "db > processing statement \"insert 1 user1 person1@example.com\"",
                "executing insert statement",
                "result Success",
                "db > processing statement \"select\"",
                "executing select statement",
                "1, \"user1\", \"person1@example.com\"",
                "db > "
            ]
        );
    };

    clean_test(test_case, test)();
}

#[test]
fn prints_error_message_when_table_is_full() {
    let test_case = "prints_error_message_when_table_is_full";
    let test = |test_file_name: &str| {
        let mut cmds: Vec<String> = (1..1402)
            .map(|i| format!("insert {} user{} person{}@example.com", i, i, i))
            .collect();
        cmds.push(".exit".into());

        let output = run_script(cmds, &test_file_name);
        let relevant_output = output.get(output.len() - 2).unwrap();
        assert_eq!(relevant_output, "db message: Execute(TableFull)",);
    };

    clean_test(test_case, test)();
}

#[test]
fn allows_inserting_and_selecting_strings_that_are_the_max_length() {
    let test_case = "allows_inserting_and_selecting_strings_that_are_the_max_length";
    let test = |test_file_name: &str| {
        let long_username: String = repeat("a").take(32).collect();
        let long_email: String = repeat("a").take(255).collect();

        let cmds = vec![
            format!("insert 1 {} {}", long_username, long_email),
            "select".into(),
            ".exit".into(),
        ];
        let output = run_script(cmds, &test_file_name);
        assert_eq!(
            output,
            vec![
                "db > processing statement \"insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"", 
                "executing insert statement",
                "result Success",
                "db > processing statement \"select\"",
                "executing select statement",
                "1, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"",
                "db > "
            ]
        );
    };
    clean_test(test_case, test)();
}

#[test]
fn prints_error_messages_if_strings_are_too_long() {
    let test_case = "prints_error_messages_if_strings_are_too_long";
    let test = |test_file_name: &str| {
        let long_username: String = repeat("a").take(33).collect();
        let long_email: String = repeat("a").take(256).collect();

        let cmds = vec![
            format!("insert 1 {} {}", long_username, long_email),
            ".exit".into(),
        ];
        let output = run_script(cmds, &test_file_name);
        assert_eq!(
            output,
                vec![
                    "db > processing statement \"insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"",
                    "db message: Statement(TooLong)",
                    "db > "
                ]
        );
    };

    clean_test(test_case, test)();
}

#[test]
fn prints_error_messages_if_id_is_negative() {
    let test_case = "prints_error_messages_if_id_is_negative";
    let test = |test_file_name: &str| {
        let long_username = "a";
        let long_email = "a";

        let cmds = vec![
            format!("insert -1 {} {}", long_username, long_email),
            ".exit".into(),
        ];
        let output = run_script(cmds, &test_file_name);
        assert_eq!(
            output,
            vec![
                "db > processing statement \"insert -1 a a\"",
                "db message: Statement(InvalidId)",
                "db > "
            ]
        );
    };

    clean_test(test_case, test)();
}

#[test]
fn keeps_data_after_closing_connection() {
    let test_case = "keeps_data_after_closing_connection";
    let test = |test_file_name: &str| {
        let output1 = run_script(
            vec!["insert 1 user1 person1@example.com".into(), ".exit".into()],
            &test_file_name,
        );
        assert_eq!(
            output1,
            vec![
                "db > processing statement \"insert 1 user1 person1@example.com\"",
                "executing insert statement",
                "result Success",
                "db > ",
            ]
        );

        // std::thread::sleep(Duration::from_millis(1000));

        let output2 = run_script(vec!["select".into(), ".exit".into()], &test_file_name);
        assert_eq!(
            output2,
            vec![
                "db > processing statement \"select\"",
                "executing select statement",
                "1, \"user1\", \"person1@example.com\"",
                "db > "
            ]
        );
    };
    clean_test(test_case, test)();
}
