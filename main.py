"""
AUTHOR: Caetano Florian Roberti
Date: 04 May 2019
"""
import update_db
import query_db
import calculate_returns
import calculate_regression
import pandas as pd


db_path = '/miniconda3/envs/ccapm_regression/Code/ccapm_regression/yahoo_financials_daily.db' # path to the Database
has_to_update_bd = 1 # 1 - if is need to update de database before; 0 - if not
price = 'adjclose' # can be 'adjclose' or 'close'
filter_zero_volume = True # filter stock prices where volume = 0, it is advised to set True


if has_to_update_bd == 1:

    print('The database will be updated, it will take a moment.\n')
    answer_terminate_update = input('Do you want to continue? Yes or No: ')
    while not (answer_terminate_update[0].upper() == 'Y' or
               answer_terminate_update[0].upper() == 'N'):
        answer_terminate_update = input('Enter Yes or No: ').upper()

    if answer_terminate_update[0].upper() == 'Y':
        print()
        print("Program will be finalized")
        print()
        exit()

    try_update_db_consumption = update_db.update_db_consumption(db_path)

    if try_update_db_consumption == False:
        print('It was not possible to update Consumption Database')
        answear_update_error_consumption = input('Do you want to terminate? Yes or No: ').upper()

        while not(answear_update_error_consumption[0].upper() == 'Y' or answear_update_error_consumption[0].upper() == 'N'):
            answear_update_error_consumption = input('Enter Yes or No: ').upper()

        if answear_update_error_consumption[0].upper() == 'Y':
            raise RuntimeError('It was not possible to update Consumption Database')

    try_update_db_stocks = update_db.update_db_stocks(db_path)

    if try_update_db_stocks == False:
        print('It was not possible to update Stocks Price Database')
        answear_update_error_stocks = input('Do you want to terminate? Yes or No: ').upper()

        while not(answear_update_error_stocks[0].upper() == 'Y' or answear_update_error_stocks[0].upper() == 'N'):
            answear_update_error_stocks = input('Enter Yes or No: ').upper()

        if answear_update_error_stocks[0].upper() == 'Y':
            raise RuntimeError('It was not possible to update Stocks Price Database')

print('Calculating...\n')

all_tickers = query_db.get_all_available_stock_table(db_path)

consumption_df = query_db.query_household_consumption_index(db_path)

stocks_prices_df = query_db.query_stock_prices(db_path, all_tickers, price, filter_zero_volume)

consumption_returns_df = calculate_returns.calculate_consumption_return(consumption_df)

stocks_returns_df = calculate_returns.calculate_stock_returns_df(stocks_prices_df)

regression_dict, regression_residues_df = calculate_regression.calculate_linear_regression(stocks_returns_df, consumption_returns_df)

regression_df = calculate_regression.generate_regression_dataframe(regression_dict)


print("""
    Welcome to CCAPM Regression \n
    It will be printed every available ticker that is possible to be estimate.\n
    Choose one or more to retrieve the Ordinary Least Square estimation of the model.\n
    Chose all the tickers you want and to stop write done\n.
    If you want all of the listed, write all availables \n
    
""")

print(all_tickers)
print()

lst_answers = []

answear = input("Write the name of the ticker: ")

if answear[0].lower() == 'd':
    print()
    print("Program will be finalized")
    print()
    exit()

if answear == 'all availables':
    queried_regression_df = regression_df
    queried_residuals_df = regression_residues_df

else:

    while answear[0].lower() != 'd':
        if answear == 'all availables':
            queried_regression_df = regression_df
            queried_residuals_df = regression_residues_df
            break

        if answear not in all_tickers:
            print()
            print(answear, ' is not a valid ticker, enter only the ones showed above\n')
            answear = input("Write the name of the ticker: ")
            continue

        if len(lst_answers) != 0: # not working!!
            if answear in lst_answers:
                continue

        lst_answers.append(answear)
        answear = input("Write the name of the ticker: ")

    if answear == 'all availables':
        queried_regression_df = regression_df
        queried_residuals_df = regression_residues_df

    else:
        queried_regression_df = regression_df.loc[lst_answers,:]
        queried_residuals_df = regression_residues_df.loc[:,lst_answers]


pd.options.display.max_rows = 999
pd.options.display.max_columns = 20

print()
print(queried_regression_df)
print()


answear_export = input('Do you want to export into a .csv? Yes or No ').upper()

while not(answear_export[0].upper() == 'Y' or answear_export[0].upper() == 'N'):
    answear_update_error_consumption = input('Enter Yes or No ').upper()

if answear_export[0].upper() == 'Y':
    answer_export_name = input("Enter the name to the file be saved with: ")
    if answer_export_name[-4:] != '.csv':
        answer_export_name_residuals = answer_export_name + '_residuals.csv'
        answer_export_name += '.csv'
    else:
        answer_export_name_residuals = answer_export_name[:-4] + '_residuals.csv'


    answear_export_col_separator = input("Enter the column separator, ; or , : ")
    while not (answear_export_col_separator == ';' or answear_export_col_separator == ','):
        answear_export_col_separator = input('Enter ; or ,: ')

    queried_regression_df.to_csv(answer_export_name, sep=answear_export_col_separator)
    queried_residuals_df.to_csv(answer_export_name_residuals, sep=answear_export_col_separator)

    print()
    print('Exported queried regression(s) and residuals with success\n')

print("Program will be finalized")















