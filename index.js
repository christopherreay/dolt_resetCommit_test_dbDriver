// Import MySQL and dotenv
require('dotenv').config();
const mysql = require('mysql');



// read from .env file
let rootDBHost      = process.env.DOLT_HOST;
let rootDBUser      = process.env.DOLT_USER;
let rootDBPassword  = process.env.DOLT_PASSWORD;



let mySQLDatabase_namespace   = "traveller.modules.remoteBranch.mySQLDatabase";

let mySQLDatabase = 
  { "createDatabaseIfNotExist": true,
    "mySQLDatabaseName"       : "testingDoltResetCommit"
  };
let mySQLDatabaseName         = mySQLDatabase.mySQLDatabaseName;
let createDatabaseIfNotExist  = mySQLDatabase.createDatabaseIfNotExist;


let locationBase_string = "@: "+mySQLDatabase_namespace+": ";


debugger;
// if there isnt authenticateNodeToAccess data, then we want to redirect to there, and add the originating thingies
// let authenticateNodeToAccess_namespace  = "traveller.modules.eventDrivenGraph.authenticateNodeToAccess";
// let authenticateNodeToAccess            = namespace.getIfExists(traveller, authenticateNodeToAccess_namespace);

let results = {"success":false};
function isString(value) {
    return typeof value === 'string';
}



(
  async () =>
  {
    try
    {
      if (! (/^[a-zA-Z0-9_]+$/.test(mySQLDatabaseName)) ) 
      {
        throw new Error(locationBase_string+'Invalid database name: '+mySQLDatabaseName);
      }
        
        
      let async_connect = 
          async (connection) =>
          {
            return await new Promise( (resolvePromise, rejectPromise) =>
                  {
                    connection.connect
                        ( (error) => 
                          {
                            if (error) rejectPromise(error);
                            resolvePromise();
                          }
                        )
                  }
                )
          }
      
      let mySQLDatabase_runtimeState = {};
      mySQLDatabase_runtimeState.controlConnection = 
            mysql.createConnection
            ( 
              { "host":     rootDBHost,
                "user":     rootDBUser,
                "password": rootDBPassword,
              }
            );
        await async_connect(mySQLDatabase_runtimeState.controlConnection);  
      
      
      
      let async_transaction = 
          async (connection, queries_arrayOfStringsOrObjects) =>
          {
            debugger;
            
            return await (new Promise( (resolvePromise, rejectPromise) =>
                  {
                    connection.beginTransaction
                        ( 
                          async (error) =>
                          {
                            if (error) 
                            {
                              rejectPromise(error)
                            }
                            else
                            {
                              let allTransactionResults = [];
                              try
                              {
                                for (let queryItem of queries_arrayOfStringsOrObjects)
                                {
                                  allTransactionResults.push(await async_query(connection, queryItem)); 
                                }
                                connection.commit();
                                resolvePromise(allTransactionResults);
                              }
                              catch (error2)
                              {
                                await connection.rollback();
                                allTransactionResults.push(error2)
                                rejectPromise(allTransactionResults);
                              }
                            }
                          }
                        );
                  }
                ));
          }
      let async_query = 
          async (connection, query_stringOrObject) =>
          {
            debugger;
            
            return await (new Promise( (resolvePromise, rejectPromise) =>
                    { 
                      
                      if (isString(query_stringOrObject))
                      {
                        query_stringOrObject = 
                            {
                              "sql": query_stringOrObject,
                              "timeout": 20000,
                            }
                      }
                      else
                      {
                        if (query_stringOrObject.timeout === undefined) query_stringOrObject.timeout = 20000;
                      }
                      
                      let toReturn = { "query": query_stringOrObject.sql.substr(0,200),  "success": false};
                      toReturn.values = JSON.stringify(query_stringOrObject?.values)?.substr(0,200);
                      
                      connection.query
                          ( query_stringOrObject,
                            (error, results) =>
                            {
                              if (error) 
                              {
                                toReturn.error = error;
                                toReturn.errorMessage = error.message;
                                rejectPromise(toReturn);
                              }
                              else
                              {
                                toReturn.results = results;
                                toReturn.success = true;
                                resolvePromise(toReturn);
                              }
                            }
                          );
                    }
                  )
                );
          }
      
      let dropDatabase =
          async (databaseName) =>
          {
            // TODO: implement this later. Will require keeping track of all the connections issued to this database,
            //   lightly removing them, and removing the relevant user from the MySQL instance, and the credentials from the applicationState
            
            // r/n its a distraction for a zero use case scenario, just for completion of the tests, and its simple, but will be a bit time consuming
          }
      
      let listOfExistingDBs = (await async_query(mySQLDatabase_runtimeState.controlConnection, "SHOW DATABASES") ).results.map(item => item.Database);
      
      // else
      {
        
        
        if (! listOfExistingDBs.includes(mySQLDatabaseName))
        {
           if (createDatabaseIfNotExist !== true)
          {
            throw new Error(locationBase_string+"Database not found, createDatabaseIfNotExist is false");
          }
          else
          {
            // let dbUser      = mySQLDatabaseName+"_primaryUser"; // Can we use the HOST field as an identifier, or does it have to be localhost?
            let dbUser      = require("crypto").createHash('sha256').update(mySQLDatabaseName+"_primaryUser").digest('hex').substring(0, 32);
            let dbPassword  = "abc123"
            
            await async_transaction
                ( mySQLDatabase_runtimeState.controlConnection,
                  [
                    {
                      "sql"   : "CREATE DATABASE "+mySQLDatabaseName,
                    },
                    {
                      "sql"   : "CREATE USER ?@'localhost' IDENTIFIED BY ?",
                      "values": [dbUser, dbPassword]
                    },
                    {
                      "sql"   : "GRANT ALL PRIVILEGES ON "+mySQLDatabaseName+".* TO ?@'localhost'",
                      "values": [dbUser]
                    },
                  ]
                );
                
            debugger;
          }
        }
        
        let dbUser      = require("crypto").createHash('sha256').update(mySQLDatabaseName+"_primaryUser").digest('hex').substring(0, 32);
        let dbPassword  = "abc123"
        
        let connectionToReturn = mysql.createConnection
            (
              {
                "host"      : "localhost",
                "database"  : mySQLDatabaseName,
                "user"      : dbUser,
                "password"  : dbPassword,
              }
            );
        
        await async_connect(connectionToReturn);
        
        results.data = 
            { connectionToReturn,
             "async_query"      : async (query_stringOrObject)            => { return await async_query(connectionToReturn, query_stringOrObject); },
             "async_transaction": async (queries_arrayOfStringsOrObjects) => { return await async_transaction(connectionToReturn, queries_arrayOfStringsOrObjects); },
            };
        debugger;
        
        
        results.success = true;
      }

      }
      catch (error)
      {
        debugger;
        results.error = error;
        results.errorMessage = error.message;
      }



      let writeToLog =
          (contentToWrite) =>
          {
            console.log(JSON.stringify(contentToWrite, null, 2));
          }


      try
      {
        
        
        
        if (results.success !== true)
        { throw new Error(results.errorMessage);
        }
        else
        {
          let databaseTools = results.data;
          
          writeToLog("connecting to database...success");
          
          debugger;
          
          let dbTableName = "testDoltTable";
          let dbNewBranch = "christopher_someNewBranchName";
          
          
          let response;
          let stepDisplayName;
          
          
          stepDisplayName = "reading HEAD commit hash of main";
          
          writeToLog(`${stepDisplayName}...`);
          response = await databaseTools.async_transaction
              ( [
                  { "sql": `SELECT hash from dolt_branches WHERE name = ?;`, "values": ["main"],
                  },
                  { "sql": `SELECT * from dolt_commits`, "values": [],
                  }
                ],
              );
          writeToLog(response);
          
          // we need to grab the commit from the response
          let dbMainInitialCommit = response[0].results[0].hash;
          writeToLog(`${stepDisplayName}...complete`);

          
          response = await databaseTools.async_transaction
              ( [
                  { sql: `CREATE TABLE IF NOT EXISTS ${dbTableName} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))`, "values": [] },
                  { sql: `INSERT INTO ${dbTableName} (name) VALUES (?)`, values: ['Alexandria'] },
                  { sql: `INSERT INTO ${dbTableName} (name) VALUES (?)`, values: ['Bruno'] }
                ]
              );
          writeToLog(response);
          
          // Query the table to ensure data was inserted
          response = await databaseTools.async_query({ "sql": `SELECT * FROM ${dbTableName}`, "values": [] });
          writeToLog(response);
          
          
          writeToLog("commiting data to main...");
          
          response = await databaseTools.async_transaction
              ( [
                  { "sql": `CALL DOLT_ADD(?)`,  "values": [dbTableName]  
                  },
                  { "sql": `CALL DOLT_COMMIT("-m", ?)`, "values": [`first commit onto main of Alexandria and Bruno. JSON(${JSON.stringify({"someMetaData": "abc123"} ) })` ],
                  }
                ],
              );
          writeToLog(response);
          writeToLog("commiting data to main...complete");
          
          
          writeToLog("creating Branch and Checking Out...");
          
          response = await databaseTools.async_transaction
              ( [
                  { "sql": `CALL DOLT_CHECKOUT('-b', ?);`,  "values": [ dbNewBranch ]
                  },
                ],
              );
          writeToLog(response);
          writeToLog("creating Branch and Checking Out...complete");
          
          
          writeToLog("writing changes to new branch...");
          
          response = await databaseTools.async_transaction
              ( [
                  { sql: `INSERT INTO ${dbTableName} (name) VALUES (?)`, values: ['Cassandra'] },
                  { sql: `INSERT INTO ${dbTableName} (name) VALUES (?)`, values: ['Diego'] }
                ],
              );
          writeToLog(response);
          response = await databaseTools.async_query({ "sql": `SELECT * FROM ${dbTableName}`, "values": [] });
          writeToLog(response);
          writeToLog("writing changes to new branch...complete");
          
          
          writeToLog("commiting data to "+dbNewBranch+"...");
          response = await databaseTools.async_transaction
              ( [
                  { "sql": `CALL DOLT_ADD(?)`,  "values": [ dbTableName ]  
                  },
                  { "sql": `CALL DOLT_COMMIT("-m", ?)`, "values": [`Commit onto ${dbNewBranch} of Cassandra and Diego` ],
                  }
                ],
              );
          writeToLog(response);
          let dbNewBranch_initialCommit = response[1].results[0].hash;
          writeToLog("commiting data to "+dbNewBranch+"...complete");
          
          
          writeToLog("generating a diff with main...");
          response = await databaseTools.async_transaction
              ( [
                  { sql: `SELECT * FROM dolt_diff(?, ?, ?);`, values: ["main", dbNewBranch, dbTableName] 
                  },
                ],
              );
          writeToLog(response);
          writeToLog("generating a diff with main...complete");
          
          
          writeToLog("merging with main...");
          response = await databaseTools.async_transaction
              ( [
                  { "sql": `SELECT * FROM dolt_branches;`,  "values": []
                  },
                  { "sql": `CALL DOLT_CHECKOUT(?);`,  "values": [ "main" ]
                  },
                  
                  // { sql: `SELECT * from dolt_commit_ancestors;`,  "values": []
                  // },
                  { "sql": `CALL DOLT_MERGE(?, ?, ?, ?);`,  "values": [ dbNewBranch, "--no-ff", "-m", JSON.stringify({"operation": "mergeProcessArc", "initialCommit": dbNewBranch_initialCommit, "mergedBranch": dbNewBranch }) ]
                  },
                  // { sql: `SELECT * from dolt_commit_ancestors;`,  "values": []
                  // },
                  // { sql: `CALL DOLT_ADD('-A', ?);`,  "values": [ dbTableName ]
                  // },
                  // { sql: `CALL DOLT_COMMIT('-m', ?);`,  "values": [ `committed merge data from ${dbNewBranch}` ]
                  // },
                ],
              );
          writeToLog(response);
          response = await databaseTools.async_query({ "sql": `SELECT * FROM ${dbTableName}`, "values": [] });
          writeToLog(response);
          writeToLog("merging with main...complete");
          
          
          stepDisplayName = "removing new branch";
          
          writeToLog(`${stepDisplayName}...`);
          response = await databaseTools.async_transaction
              ( [
                  { sql: `CALL DOLT_BRANCH('-d', ?)`,  "values": [ dbNewBranch ]
                  },
                  { sql: `SELECT * FROM dolt_branches;`,  "values": []
                  },
                  { sql: `SELECT * from dolt_commit_ancestors;`,  "values": []
                  },
                ],
              );
          writeToLog(response);
          writeToLog(`${stepDisplayName}...complete`);
          
          
          stepDisplayName = "reset --hard to initial commit";
          
          writeToLog(`${stepDisplayName}...`);
          response = await databaseTools.async_transaction
              ( [
                  { sql: `CALL DOLT_RESET('--hard', ?)`, values: [dbMainInitialCommit],
                  },
                  { sql: `CALL DOLT_COMMIT('-m', ?)`, values: ["reset db to initial state for testing"],
                  },
                  { sql: `SELECT * from dolt_commits;`, values: [],
                  },
                ],
              );
          writeToLog(response);
          writeToLog(`${stepDisplayName}...complete`);
        }
      }
      catch (error)
      {
        writeToLog({error, "message":error.message});
      }
                

      
    

  }
)();




