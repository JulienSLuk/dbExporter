import pypyodbc as odbc # pip install pypyodbc
import csv
import os
import time

# Current directory of python script
current_dir = os.getcwd()

def read_csv(filepath):
    items = []
    filename = os.path.basename(filepath)

    with open(filepath, 'r') as file:
        reader = csv.reader(file)
        next(reader, None) # skip the header row

        for row in reader:  # for each row in csv
            if len(row) != 0:  # Check if the row is not empty
                if filename == "query.csv":  # If working with csv of query
                    line_as_str = ','.join(row)
                    items.append(line_as_str)
                else:
                    for cell in row:
                        if isinstance(cell, str):  # Check if it is a string
                            items.append(cell.strip())  # Remove all trailing whitespace at right side
                        else:
                            continue
            else:
                continue
            
    return items


def get_csv(current_dir_with_csv_folder):
    dbDetails_path = f"{current_dir_with_csv_folder}\\dbDetails.csv"  # Database Info
    db_Details = read_csv(dbDetails_path)

    confDetails_path = f"{current_dir_with_csv_folder}\\confDetails.csv"  # Confluence Info
    conf_Details = read_csv(confDetails_path)

    query_path = f"{current_dir_with_csv_folder}\\query.csv"  # Database queries
    query_info = read_csv(query_path)

    return db_Details, conf_Details, query_info


# Get names of csv files within a folder location
def get_csv_filenames(folder_path):
    file_list = os.listdir(folder_path)
    csv_file_names = [file for file in file_list if file.endswith('.csv')]
    return csv_file_names


def create_log_csv(exception_file_name):
    # Create exception file to contain exception e message
    exception_csv_fp = os.path.join(exported_folder_path, exception_file_name)
    print(f"An error occurred. Check {exception_csv_fp} for exception log details.")
    with open(exception_csv_fp, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([type(e).__name__, str(e)])
    time.sleep(10)


# All csv to be used
csv_folder_name = "Config"
current_dir_with_config_folder = os.path.join(current_dir, csv_folder_name)
db_Details, conf_Details, query_info = get_csv(current_dir_with_config_folder)


# Create the exported folder if it doesn't exist
exported_folder_path = os.path.join(current_dir, "exported")
if not os.path.exists(exported_folder_path):
    os.makedirs(exported_folder_path)


# Iterate over the queries in query.csv file
for query in query_info:

    # Create a file name for the exported CSV file
    csv_file_name = "exported_query.csv"

    # Create the full path to the exported CSV file
    csv_file_path = os.path.join(exported_folder_path, csv_file_name)

    driver_name = db_Details[0]
    server_name = db_Details[1]
    database_name = db_Details[2]
    db_user = db_Details[3]
    db_pass = db_Details[4]

    print("Attempting Connection...")
    connection_string = f"""
        DRIVER={{{driver_name}}};
        SERVER={server_name};
        DATABASE={database_name};
        Trust_Connections=yes;
        uid={db_user};
        pwd={db_pass};
    """

    try:
        conn = odbc.connect(connection_string)

        print("Connected to the database, querying data...")

        # Execute the query and get the column names
        cursor = conn.cursor()
        cursor.execute(query)  # Error

        print("Data queried, exporting to csv...")

        column_names = [column[0] for column in cursor.description]

        # Create a CSV file
        with open(csv_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)

            # Write the header row
            writer.writerow(column_names)

            # Write the results of the query to the CSV file
            for row in cursor.fetchall():
                writer.writerow(row)     

        # Close the CSV file
        csvfile.close()

        print("Succesfully exported to csv!")

    except Exception as e:
        # Create exception file to contain exception e message
        exception_file_name = "exception_e"
        create_log_csv(exception_file_name)

    finally:
        cursor.close()
        conn.close()


# Get csv data.
export_folder_name = "exported"
current_dir_with_exported_folder = os.path.join(current_dir, export_folder_name)
csv_file_name_list = get_csv_filenames(current_dir_with_exported_folder)