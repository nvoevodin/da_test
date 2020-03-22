Task
----

Return the most recent housing status and housing status update date for all
clients who were active as of 5-15-2017. It is safe to assume that a
client is still active if s/he does not have a subsequent inactive
program status update.

Summary
-------

Here are the results:

-   IDS ‘JKL’, ‘STU’, and ‘PQR’ were active as of ‘2017-05-15’ and their
    latest housing status is ‘unhoused’;
-   ID ‘YZA’ was active and is housed;
-   ID ‘VWX’ was active but is missing status;
-   ID ‘GHI’ was active but not present at the housing status sheet;
-   ID ‘STU’ is active but is missing program status date (should look
    into it)

Here is the final table:

    ##    Client_ID Program_status Housing_Status Housing_Status_Date
    ## 1:       VWX         Active missing_status          2017-05-15
    ## 2:       YZA         Active         Housed          2017-05-15
    ## 3:       JKL         Active       Unhoused          2017-06-06
    ## 4:       STU         Active       Unhoused          2017-06-15
    ## 5:       PQR         Active       Unhoused          2017-07-07
    ## 6:       GHI         Active

Besides faulty values, there were a couple more things that could be
fixed:

-   date formats from mdy to ymd;
-   column names should all be lower case or at least uniform.

Analysis
--------

I loaded the data into mySQL test database on AWS so I could show you
that I can do both R and SQL, and also for convenience.

Below, you will find 3 different solutions with the same results.

1.  Straight SQL query
2.  R with data pulled from SQL
3.  R with data pulled from CSVs

Note: If you are behind DOITT firewall, the AWS SQL connection might get
blocked. You can either switch to a regular WIFI or just go straight to
the 3rd solution (R from CSVs).

### Libraries

Installing some of the packages that we might or might not need. I wrote
this ‘installer’ function some time ago to automate library
installations. It checks if a library is installed and if yes, it will
load it. If not, it will install it and then load it.

    installer <- function(x){
      for( i in x ){
        if( !require( i , character.only = TRUE ) ){
          #  If not loading - install
          install.packages( i , dependencies = TRUE )
          #  Load 
          require(i, character.only = TRUE )
        }
      }
    }

    installer( c("dplyr" , "data.table" , "RMySQL", 'lubridate', 'pbapply') )

### SQL Solution

    # Connecting to the DB:
    connection = dbConnect(drv = MySQL(), #specifying database type. 
                           user = "test", # username
                           password = 'testtest', # password
                           host = 'nikitatest.ctxqlb5xlcju.us-east-2.rds.amazonaws.com', # address
                           port = 3306, # port
                           dbname = 'nikita99') # name of the database

The query below selects required fields from two derived tables that I
joing together using LEFT JOIN. The derived tables are aggregations that
return required statues.

    resultSQL <- dbGetQuery(connection,
    "SELECT 
    program.client_id,
    program.program_status,
    housing.housing_status_date,
    housing.housing_status
    FROM        
    (SELECT 
        table1.client_id, 
        IFNULL(table1.program_status_date, 'missing_date') AS program_status_date,
        table1.program_status 
    FROM nikita99.test_program_status as table1 
      INNER JOIN (SELECT 
                      client_id, 
                      MAX(IFNULL(program_status_date, 'missing_date')) AS lastDate
                  FROM nikita99.test_program_status
                  WHERE program_status_date <= '2017-05-15'
                  GROUP BY client_id) AS table2 
        ON table2.client_id = table1.client_id 
        AND table2.lastDate = IFNULL(table1.program_status_date, 'missing_date')
        WHERE program_status IN ('missing_date','active')) AS program   
    LEFT JOIN
        (SELECT 
        table3.client_id, 
        table3.housing_status_date,
        IF(table3.housing_status = '', 'missing_status', table3.housing_status) AS housing_status
    FROM nikita99.test_housing_status as table3 
      INNER JOIN (SELECT 
                      client_id, 
                      max(housing_status_date) AS lastDate
                  FROM nikita99.test_housing_status
                 
                  GROUP BY client_id) AS table4 
        ON table4.client_id = table3.client_id 
        AND table4.lastDate = table3.housing_status_date)
    AS housing
    ON housing.client_id = program.client_id
    ")
    # Printing the result
    print(resultSQL)

    ##   client_id program_status housing_status_date housing_status
    ## 1       JKL         Active          2017-06-06       Unhoused
    ## 2       PQR         Active          2017-07-07       Unhoused
    ## 3       VWX         Active          2017-05-15 missing_status
    ## 4       YZA         Active          2017-05-15         Housed
    ## 5       GHI         Active                <NA>           <NA>

### R Solution

    # Pulling program status table
    program <- dbGetQuery(connection,
                          "SELECT *
                          FROM test_program_status")

    # Pulling housing status table
    housing <- dbGetQuery(connection,
                          "SELECT *
                          FROM test_housing_status")

    # Renaming faulty data
    program <- setDT(program)[is.na(Program_status_date), Program_status_date:='0000-00-00']

    # Using dplyr piping to find the last 
    # status by id before or including
    # the requested date, and then keep it if it was 'active'.
    program <- program %>%
      dplyr::filter(Program_status_date <= '2017-05-15') %>%
      dplyr::group_by(Client_ID) %>% 
      dplyr::arrange(Client_ID, desc(Program_status_date)) %>%
      dplyr::top_n(1) %>%
      dplyr::filter(Program_status == 'Active')

    # Keeping the last housing status by id
    housing <- housing %>% 
      dplyr::group_by(Client_ID) %>% 
      dplyr::arrange(Client_ID, desc(Housing_Status_Date)) %>%
      dplyr::top_n(1)

    # Renaming faulty data
    housing <- setDT(housing)[Housing_Status == '', Housing_Status:='missing_status']

    # Left joining housing statuses to program statuses to see 
    # the housing situation of the active ids. Keeping only relevant
    # columns and arranging by date
    resultR <- dplyr::left_join(program,housing)[,c(1,2,4,5)] %>% 
      dplyr::arrange(Housing_Status_Date)

    # Printing
    print(resultR)

    ## # A tibble: 6 x 4
    ## # Groups:   Client_ID [6]
    ##   Client_ID Program_status Housing_Status Housing_Status_Date
    ##   <chr>     <chr>          <chr>          <chr>              
    ## 1 VWX       Active         missing_status 2017-05-15         
    ## 2 YZA       Active         Housed         2017-05-15         
    ## 3 JKL       Active         Unhoused       2017-06-06         
    ## 4 STU       Active         Unhoused       2017-06-15         
    ## 5 PQR       Active         Unhoused       2017-07-07         
    ## 6 GHI       Active         <NA>           <NA>

    # Disconnecting from the DB
    dbDisconnect(connection)

    ## [1] TRUE

### R from CSVs

    # Pulling data from CSVs
    housingCSV <- fread('test_housing_status.csv')
    programCSV <- fread('test_program_status.csv')

    # Dates are messed up. Transforming them to ymd format
    programCSV$Program_status_date <- as.character(lubridate::mdy(programCSV$Program_status_date))
    housingCSV$Housing_Status_Date <- as.character(lubridate::mdy(housingCSV$Housing_Status_Date))

    # Renaming faulty data
    programCSV <- setDT(programCSV)[is.na(Program_status_date), Program_status_date:='0000-00-00']

    # Using dplyr piping to find the last 
    # status by id before or including
    # the requested date, and then keep it if it was 'active'.
    programCSV <- programCSV %>% 
      dplyr::filter(Program_status_date <= '2017-05-15') %>%
      dplyr::group_by(Client_ID) %>% 
      dplyr::arrange(Client_ID, desc(Program_status_date)) %>%
      dplyr::top_n(1) %>%
      dplyr::filter(Program_status == 'Active')

    # Keeping the last housing status by id before or including
    # the requested date.
    housingCSV <- housingCSV %>% 
      dplyr::group_by(Client_ID) %>% 
      dplyr::arrange(Client_ID, desc(Housing_Status_Date)) %>%
      dplyr::top_n(1)

    # renaming empty strings into readable format
    housingCSV <- setDT(housingCSV)[Housing_Status == '', Housing_Status:='missing_status']

    # Left joining housing statuses to program statuses to see 
    # the housing situation of the active ids. Keeping only relevant
    # columns and arranging by date
    resultR_CSV <- dplyr::left_join(programCSV, housingCSV)[,c(1,2,4,5)] %>% 
      dplyr::arrange(Housing_Status_Date)

    # Printing
    print(resultR_CSV)

    ## # A tibble: 6 x 4
    ## # Groups:   Client_ID [6]
    ##   Client_ID Program_status Housing_Status Housing_Status_Date
    ##   <chr>     <chr>          <chr>          <chr>              
    ## 1 VWX       Active         missing_status 2017-05-15         
    ## 2 YZA       Active         Housed         2017-05-15         
    ## 3 JKL       Active         Unhoused       2017-06-06         
    ## 4 STU       Active         Unhoused       2017-06-15         
    ## 5 PQR       Active         Unhoused       2017-07-07         
    ## 6 GHI       Active         <NA>           <NA>

    fwrite(resultR_CSV, 'final_result.csv')

THE END
=======
