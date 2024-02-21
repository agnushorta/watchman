use tokio::runtime::Runtime;
use chrono::prelude::*;
use std::path::Path;

async fn grab_data() -> Result<String, Box<dyn std::error::Error>> {
    let body = reqwest::get("http://strickland:9090/api/v1/query?query=mrR_posmgr_spread{status='FINISHED', type=~'R[A-Z]*'}")
    .await?
    .text()
    .await?;

    //println!("body = {:?}", body);
    Ok(body)
}

fn main() {

    let mut data: String = String::new();

    let path = Path::new("./metrics.db");
    let connection = sqlite::open(path).unwrap();

    if path.exists() {
        println!("Database does not exist");
        let query = "
            CREATE TABLE metrics(robot_id TEXT, user_id TEXT, rtype TEXT, value REAL, timestamp TEXT);
        ";
        connection.execute(query).unwrap();
    } else {
        println!("Database exists");
    }

    // call async function to grab data
    match Runtime::new() 
        .expect("Failed to create Tokio runtime")
        .block_on(grab_data())
        {
            Ok(sk) => data = String::from(sk),
            Err(e) => println!("Error: {:?}", e)
        }

    if data.len() > 0 {
        //println!("Data: {:?}", data);
        let parsed = json::parse(&data).unwrap();
        //println!("Data: {:?}", parsed);
        println!("Length: {:?}", parsed["data"]["result"].len());
        /*
            {'metric':
                {'__name__':'mrR_posmgr_spread',
                 'exported_instance':'20434',
                 'exported_job':'mr_robot',
                 'instance':'localhost:9091',
                 'job':'mrRobot',
                 'robot_id':'20434',
                 'status':'FINISHED',
                 'stranded':'F',
                 'type':'RMARKET',
                 'user_id':'137096'
                },
                'value':[1708364903.357,'0']
            }
         */
        for i in 0..parsed["data"]["result"].len() {
            /*
            println!("Robot ID: {:?} User ID: {:?} Type: {:?} Value: {:?}", 
                parsed["data"]["result"][i]["metric"]["robot_id"], 
                parsed["data"]["result"][i]["metric"]["user_id"],
                parsed["data"]["result"][i]["metric"]["type"],
                parsed["data"]["result"][i]["value"][1]);
            */
            let robot_id = parsed["data"]["result"][i]["metric"]["robot_id"].as_str().unwrap();
            let user_id = parsed["data"]["result"][i]["metric"]["user_id"].as_str().unwrap();
            let rtype = parsed["data"]["result"][i]["metric"]["type"].as_str().unwrap();
            let value = parsed["data"]["result"][i]["value"][1].as_str().unwrap();
            let utc: DateTime<Utc> = Utc::now();  
            println!("Robot_id: {:?} User_id: {:?} RType: {:?} Value: {:?} Timestamp: {:?}", 
                robot_id.parse::<i64>().unwrap(),
                user_id.parse::<i64>().unwrap(),
                rtype.parse::<String>().unwrap(),
                value.parse::<f64>().unwrap(),
                utc.to_rfc3339()
            );
            //let query = "INSERT INTO metric VALUES (?1, ?2, ?3, ?4, ?5)";
            let query = format!("INSERT INTO metrics VALUES ('{}', '{}', '{}', {}, '{}')", robot_id, user_id, rtype, value, utc.to_rfc3339());
            connection.execute(query).unwrap();
        }
    } else {
        println!("No data returned");
    }
}
