# Standard library imports
import os

# Third party imports
from dotenv import load_dotenv
import gspread
import gspread_dataframe as gd
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# Local imports


class GoogleSheets:
    """
    Use me to interact with the Google Sheets API. To ensure a successful connection, please give the email address
    associated with your GCP service account edit access to your Google Sheets spreadsheet.
    Pass in the id of a google sheets spreadsheet to instantiate a specific google sheets object to manipulate with the following methods:
        - `import_data_to_google_sheets`: Imports a Pandas DataFrame into a target Google Sheets worksheet.
        - `write_string_to_cell`: Writes a string into the specified cell of a worksheet.
        - `delete_sheet`: Deletes a specified worksheet inside of a Google Sheets spreadsheet.
        - `list_all_sheets`: Lists all worksheets present in a Google Sheets spreadsheet.

    Defintions:
        - A `spreadsheet` as it relates to this class is a broader Google Sheets workbook in which sheets or tabs are stored.
        - A `worksheet` as it relates to this class is an individual sheet or tab inside of a broader Google Sheets workbook.
    """
    
    def __init__(self, google_sheet_spreadsheet_id:str):
        load_dotenv()

        self.sa_path = os.getenv('sa_path')

        self.scopes = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive"]

        self.google_creds = ServiceAccountCredentials.from_json_keyfile_name(filename=self.sa_path, scopes=self.scopes)

        self.google_client = gspread.authorize(self.google_creds)
        
        self._enter_spreadsheet(google_sheet_spreadsheet_id)

    
    def _enter_spreadsheet(self, google_sheet_spreadsheet_id:str):
        """Enter the target Google Sheets spreadsheet."""
        self.spreadsheet = self.google_client.open_by_key(google_sheet_spreadsheet_id)


    def import_df_to_google_sheet(
            self,
            dataframe:pd.DataFrame,
            google_sheet_worksheet_name:str,
            clear_and_resize_sheet:bool=False,
            add_rows_to_bottom_of_sheet:int=1,
            starting_column:int=1,
            starting_row:int=1,
            resize_to_exact_width:bool=False
        ):

        """
        Imports a Pandas Dataframe into a target Google Sheets worksheet.

        Parameters
        ----------
            dataframe (tuple): A dataframe object.
            google_sheet_spreadsheet_id (str): ID of the target Google Sheets spreadsheet.
            google_sheet_worksheet_name (str): Name of the target worksheet inside the target Google Sheets spreadsheet.
            clear_and_resize_sheet (bool): Boolean value determining whether the function should clear all cells in target worksheet and resize the sheet to the desired length and width.
            add_rows_to_bottom_of_sheet (int): Number of rows to add to the bottom of the target worksheet.
            starting_column (int): Column number to anchor the data import.
            starting_row (int): Row number to anchor the data import.
            resize_to_exact_width (bool): Boolean value determing whether the function should resize the target worksheet to the exact width of the import. Typically leveraged to maximize worksheet cell counts for jobs with numerous imports.
        """

        self._enter_worksheet(google_sheet_worksheet_name)

        self.dataframe = dataframe.dropna(how='all')

        if clear_and_resize_sheet == True:
            self.sheet.clear()
            self.sheet.resize(rows=len(dataframe) + add_rows_to_bottom_of_sheet)
            
        if resize_to_exact_width == True:
            self.sheet.resize(cols=len(dataframe.axes[1]))

        gd.set_with_dataframe(
            self.sheet,
            self.dataframe,
            row=starting_row,
            col=starting_column
        )


    def write_string_to_cell(
            self,
            string_to_import:str,
            google_sheet_worksheet_name:str,
            cell:str='A1'
        ):

        """
        Writes a string into the specified cell of a worksheet.

        Parameters
        ----------
            string to import: A string literal
            google_sheet_spreadsheet_id (str): ID of the target Google Sheets spreadsheet.
            google_sheet_worksheet_name (str): Name of the target worksheet inside the target Google Sheets spreadsheet.
            cell (str): Target cell to import a string to. If parameter is not passed, the default anchoring cell is A1.
        """

        self._enter_worksheet(google_sheet_worksheet_name)

        self.sheet.update_acell(cell, string_to_import)

    
    def fetch_sheet_as_dataframe(self, google_sheet_worksheet_name:str) -> pd.DataFrame:
        """Returns the values of all cells in a worksheet as a dataframe"""

        self._enter_worksheet(google_sheet_worksheet_name)
        
        self.contents = self.sheet.get_all_records()

        self.contents_df = pd.DataFrame(data=self.contents, columns=list(self.contents[0].keys()))

        self.contents_df.dropna(how="all")

        return self.contents_df
    

    def _enter_worksheet(self, google_sheet_worksheet_name:str):
        """Enter the target Google Sheets spreadsheet and further enter a target worksheet, or create it if it does not already exist."""

        try:
            self.sheet = self.spreadsheet.worksheet(google_sheet_worksheet_name)

        except gspread.exceptions.WorksheetNotFound:
            self.sheet = self.spreadsheet.add_worksheet(title=google_sheet_worksheet_name, rows="100", cols="24")


    def delete_sheet(self, google_sheet_worksheet_name):
        """Deletes a specified worksheet inside of a Google Sheets spreadsheet."""

        # the del_worksheet() method MUST be called on a Worksheet object, not a raw string,
        # hence the first step is entering a worksheet to generate the sheet variable which
        # is a Worksheet object. Then we call the del_worksheet method on the Spreadsheet 
        # object with the Worksheet object as the parameter.
        
        self._enter_worksheet(google_sheet_worksheet_name)

        self.spreadsheet.del_worksheet(self.sheet)

    
    def list_all_sheets(self, return_as_list=False):
        """Lists all worksheets present in the Google Sheets spreadsheet."""

        if return_as_list:
            self.sheet_names_list = []
            self.sheets = self.spreadsheet.worksheets()

            for sheet in self.sheets:
                # if further splitting on ' or " characters, we might need to revisit this and remove ' characters altogether
                if '"' in str(sheet):
                    sheet_name = str(sheet).split('"')[1]
                    self.sheet_names_list.append(sheet_name)

                else:
                    sheet_name = str(sheet).split("'")[1]
                    self.sheet_names_list.append(sheet_name)
            
            return self.sheet_names_list
        
        else:
            return self.spreadsheet.worksheets()