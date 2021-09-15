use std::io::{BufRead, BufReader, Read, Write};
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
    // let mut outputs = vec![];
    cmds.for_each(|cmd| {
        stdin
            .write_all(&[cmd.as_bytes(), b"\n"].concat())
            .expect("Failed to write to stdin");

        let mut buf = vec![0u8; 100]; 
        while let Ok(n) = stdout.read(&mut buf) {   
            if n > 0 {
                println!("read bytes {:?} with length {:?}", String::from_utf8_lossy(&buf[..n]), n);    
            } else {
                println!("read nothing");    
            }
        }
        println!("output buf for last command {:?} with length {:?}", buf, buf.len());
        // outputs.push(lines);
        
    });

    let _ = child.wait().expect("Failed to read stdout");
    // outputs.concat()
    vec![]
}

#[test]
fn database_inserts_and_retrieves_a_row() {
    let output = run_script(&["insert 1 g g", "select", ".exit"]);
    assert_eq!(
        output,
        vec![
            "db > processing statement \"insert 1 g g\"",
            "executing insert statement",
            "result Success",
            "db > processing statement \"select\"",
            "executing select statement",
            "1, \"g\", \"g\"",
            "db > "
        ]
    );
}

// #[test]
// fn prints_error_message_when_table_is_full() {
//     let insert_cmds: Vec<String> = (1..208)
//         .map(|i| format!("insert {} user{} person{}@example.com", i, i, i))
//         .collect();

//     println!("inserting {:?} rows", insert_cmds.len());

//     let cmds = [insert_cmds, vec![".exit".into()]].concat();
//     let cmds: Vec<&str> = cmds.iter().map(|s| s.as_str()).collect();
//     let output = run_script(&cmds);
//     assert_eq!(output, vec![""]);
// }
