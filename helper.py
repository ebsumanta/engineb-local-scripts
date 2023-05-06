import pandas as pd

def get_csv_headers(csv_file_name):
    try:
        df = pd.read_csv(f"{csv_file_name}")
        print(df.columns)
    except Exception as ex:
        print(str(ex))




if __name__ == '__main__':
    get_csv_headers('./daa/input/cdm/glDetail.csv')