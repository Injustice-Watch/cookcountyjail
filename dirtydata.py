import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
import datetime
import dateparser
import dateutil
import pprint

typ_dict = {
    1: "SERVING SENTENCE",
    2: "PRETRIAL, FAILURE TO PLACE",
    3: "PRETRIAL, NO BOND",
    4: "PRETRIAL, FAILURE TO PAY",
    5: "WARRANT",
    6: "UNFIT AWAITING PLACEMENT",
    7: "NO REASON TO HOLD",
    8: "STATUS OF ALL CHARGES UNKNOWN"
    }

#fix original headings
#import original headings dictionary as separate object?
#then go through rest of class and substitute references to dictionary where other means have been used to refer to column headings

lastname = "Last Name"
firstname = "First Name"
iid = "Inmate ID"
bid = "Booking ID"
age_book = "Age at Booking"
age = "Age at Access"
DOB = "DOB"
city = "City"
bond = "Bond for Charge"
admit = "Last Admission Date"
no_bond = "Without Bail" #post-processing
days_in = "Days in Custody from Last Admission" #add to df and orig headings!!!!
zipcode = "Zip" #add to df and orig headings!
charge_disp = "Case Disposition"
race = "Race" #added from dictionary
gender = "Gender" #added from dictionary
statute = "Statute Violated" #replaced later
charge_desc = "Charge Description" #replaced later
charge_class = "Charge Class" #replaced later
totalbond = "Total Bond" #created in post-processing
charge_rank = "Charge Rank"
disposition_type = "Disposition Type" #option to add at beginning for corrections?
type_list = "Disposition Type List"
indices = "Indices"
charge_list = "Charges"
highest_charge = "Highest Charge"
highest_charge_class = "Class of Highest Charge"
number_charges = "Number of Charges"
charge_rank_dict = {
    "FM" : 13,
    "FX" : 12,
    "F1" : 11,
    "F2" : 10,
    "F3" : 9,
    "F4" : 8,
    "MA" : 7,
    "MB" : 6,
    "MC" : 5,
    "PETTY" : 4,
    "BUSINESS" : 3,
    "LOCAL" : 2,
    "NONE": 2,
    "CONTEMPT" : 2,
    "NO CHARGE" : -1
    }

class DirtyData:

    def __init__(self, filepath, access_date, heading_filename, source_type = "csv",
        crime_class_source = None, has_index=False, index=None,
        has_col_head=True, names=None, no_bond_indicator = "Total", crimes_standardized=False):

        self.o = __import__(heading_filename)
        self.crimes_standardized = crimes_standardized
        self.origin_file = filepath
        self.source_type = source_type
        self.replace_dict = {}
        self.main_terms = {}
        self.cat_dict = {}
        self.log = []
        self.individual_dict = {}
        self.date = dateparser.parse(access_date)
        self.df = pd.DataFrame([])

        if self.o.case_disp:
            self.case_disp = self.o.case_disp
        else:
            self.case_disp = None

        # prepare read_csv
        if source_type == "csv":
            if not has_col_head:
                if has_index and index:
                    try:
                        index_col = int(index)
                    except ValueError:
                        index_col = cl.column_to_number
                    self.original_df = pd.read_csv(filepath, index_col=index_col, header=None, names=names)
                elif has_index:
                    # assume index at 0
                    self.original_df = pd.read_csv(filepath, index_col=0, header=None, names=names)
                else:
                    self.original_df = pd.read_csv(filepath, header=None, names=names)
            elif has_col_head:
                if has_index and index:
                    try:
                        index_col = int(index)
                    except ValueError:
                        index_col = cl.column_to_number
                    self.original_df = pd.read_csv(filepath, index_col=index_col)
                elif has_index:
                    # assume index at 0
                    self.original_df = pd.read_csv(filepath, index_col=0)
                else:
                    self.original_df = pd.read_csv(filepath)
        elif source_type =="df":
            self.original_df = filepath
        else:
            print("Invalid source type")

        #check for columns and create new df

        # Last Name
        if self.o.lastname:
            self.df[lastname] = self.original_df[self.o.lastname]
        else:
            self.df[lastname] = ""

        # First Name
        if self.o.firstname:
            self.df[firstname] = self.original_df[self.o.firstname]
        else:
            self.df[firstname] = ""

        # DOB
        if self.o.DOB:
            self.df[DOB] = self.convert_dates(self.o.DOB, self.original_df)
            self.original_df[self.o.DOB] = self.convert_dates(self.o.DOB, self.original_df)
        else:
            self.df[DOB] = ""

        # Add column for age at time data was accessed
        if self.o.age:
            self.df[age] = self.original_df[self.o.age]
        elif self.o.DOB:
            self.df[age] = self.calculate_time_column(self.original_df[self.o.DOB], self.date, target_type = "single")
        else:
            self.df[age] = ""

        # Booking date
        if self.o.admit:
            self.original_df[self.o.admit] = self.convert_dates(self.o.admit, self.original_df)
            self.df[admit] = self.convert_dates(self.o.admit, self.original_df)
        else:
            self.df[admit] = ""

        # Add column for age at booking
        if self.o.age_book:
            self.df[age_book] = self.original_df[self.o.age_book]
        elif self.o.DOB and self.o.admit:
            self.df[age_book] = self.calculate_time_column(self.original_df[self.o.DOB], self.original_df[self.o.admit], target_type = "multi")
        else:
            self.df[age_book] = ""

        # Days in Custody
        if self.o.days_in:
            self.df[days_in] = self.original_df[self.o.days_in]

        elif self.o.admit:
            self.df[days_in] = self.calculate_time_column(self.original_df[self.o.admit], self.date, target_type = "single", measure = "days")

        # Race
        if self.o.race_dict and self.o.race:
            self.set_replacedict(race, self.o.race_dict)
            self.uppercase(self.o.race, self.original_df)
            self.df[race] = self.original_df[self.o.race].replace(to_replace=self.replace_dict[race])
        
        elif self.o.race:
            self.uppercase(self.o.race, self.original_df)
            self.df[race] = self.original_df[self.o.race]

        else:
            self.df[race] = ""

        # Gender
        if self.o.gender_dict and self.o.gender:
            self.set_replacedict(gender, self.o.gender_dict)
            self.uppercase(self.o.gender, self.original_df)
            self.df[gender] = self.original_df[self.o.gender].replace(to_replace=self.replace_dict[gender])
        elif self.o.gender:
            self.uppercase(self.o.gender, self.original_df)
            self.df[gender] = self.original_df[self.o.gender]

        else:
            self.df[gender] = ""

        # City
        if self.o.city_dict and self.o.city:
            self.build_replacement_dict(city, self.o.city_dict)
            self.uppercase(self.o.city, self.original_df)
            self.df[city] = self.original_df[self.o.city].replace(to_replace=self.replace_dict[city])
        elif self.o.city:
            self.uppercase(self.o.city, self.original_df)
            self.df[city] = self.original_df[self.o.city]
        else:
            self.df[city] = ""

        # Zip Code

        if self.o.zipcode:
            self.df[zipcode] = self.original_df[self.o.zipcode]
        else:
            self.df[zipcode] = ""    

        # Individual IDs
        if self.o.iid:
            self.df[iid] = self.original_df[self.o.iid]
        else:
            self.df[iid] = self.create_ids()            

        # Case Disposition

        if self.o.disp_dict:
            self.uppercase(self.o.charge_disp, self.original_df)
            self.df[charge_disp] = self.original_df[self.o.charge_disp].replace(self.o.disp_dict)
        elif self.o.charge_disp:
            self.uppercase(self.o.charge_disp, self.original_df)
            self.df[charge_disp] = self.original_df[self.o.charge_disp]

        else:
            self.df[charge_disp] = ""

        # Booking ID
        if self.o.bid:
            self.df[bid] = self.original_df[self.o.bid]

        else:
            self.df[bid] = ""
        
        #Total Bond
        if self.o.totalbond:
            self.original_df[self.o.totalbond].fillna(value=0, inplace=True)
            self.df[totalbond] = self.numbify_currency(self.o.totalbond, self.original_df)
            if no_bond_indicator == "Total":
                self.df[no_bond] = self.detect_bond_denial(totalbond, self.df)
        else:
            self.df[totalbond] = ""

        # Bond for Individual charge

        if self.o.bond:
            self.original_df[self.o.bond].fillna(value=0, inplace=True)
            self.original_df.to_csv("Bexar_County_detect_Issues.csv", index=False)
            self.df[bond] = self.numbify_currency(self.o.bond, self.original_df)
            if no_bond_indicator == "Individual":
                self.df[no_bond] = self.detect_bond_denial(bond, self.df)
        else:
            self.df[bond] = ""

        # Standardize Crime

        if crime_class_source:
            self.read_and_standardize_crimes(crime_class_source)

        elif self.o.charge_class or self.o.charge_desc or self.o.statute:

            # Class
            if self.o.charge_class_dict:
                self.build_replacement_dict(charge_class, self.o.charge_class_dict)
                self.uppercase(self.o.charge_class, self.original_df)
                self.df[charge_class] = self.original_df[self.o.charge_class].replace(to_replace=self.replace_dict[charge_class])
            elif self.o.charge_class:
                self.uppercase(self.o.charge_class, self.original_df)
                self.df[charge_class] = self.original_df[self.o.charge_class]
            else:
                self.df[charge_class] = ""

            #Charge
            if self.o.charge_desc_dict:
                self.build_replacement_dict(charge_desc, self.o.charge_desc_dict)
                self.uppercase(self.o.charge_desc, self.original_df)
                self.df[charge_desc] = self.original_df[self.o.charge_desc].replace(to_replace=self.replace_dict[charge_desc])
            elif self.o.charge_desc:
                self.uppercase(self.o.charge_desc, self.original_df)
                self.df[charge_desc] = self.original_df[self.o.charge_desc]
            else:
                self.df[charge_desc] = ""

            #Statute
            if self.o.statute_dict:
                self.build_replacement_dict(statute, self.o.statute_dict)
                self.uppercase(self.o.statute, self.original_df)
                self.df[statute] = self.original_df[self.o.statute].replace(to_replace=self.replace_dict[statute])
            elif self.o.statute:
                self.uppercase(self.o.statute, self.original_df)
                self.df[statute] = self.original_df[self.o.statute]
            else:
                self.df[statute] = ""

        if self.case_disp:
            self.init_individuals()
            self.post_process_individuals()

        else:
            print("Could not create dictionary of individuals. Please define dictionaries")

    def detect_bond_denial(self, column, df):
        bond_denied_column = []
        for item in df[column]:
            if self.o.bond_denied(item):
                bond_denied_column.append(True)
            else:
                bond_denied_column.append(False)
        return bond_denied_column

    def create_ids(self):
        if self.o.DOB:
            age_measure = DOB

        elif self.o.age:
            age_measure = age

        elif self.o.age_book:
            age_measure = age_book

        else:
            age_measure = None

        identifiers = []

        for i, row in self.df.iterrows():
            temp_id = []
            if age_measure:
                if pd.notnull(row[age_measure]):
                    temp_id.append(str(row[age_measure].month) + str(row[age_measure].day) + str(row[age_measure].year))

            if self.o.admit:
                if pd.notnull(row[admit]):
                    temp_id.append(str(row[admit].month) + str(row[admit].day) + str(row[admit].year))

            if self.o.race:
                if pd.notnull(row[race]):
                    temp_id.append(row[race])                

            if self.o.gender:
                if pd.notnull(row[gender]):
                    temp_id.append(row[gender])   

            if self.o.firstname:
                if pd.notnull(row[firstname]):
                    temp_id.append(row[firstname])

            if self.o.lastname:
                if pd.notnull(row[lastname]):
                    temp_id.append(row[lastname])

            if self.o.city:
                if pd.notnull(row[city]):
                    temp_id.append(row[city])                

            if len(identifiers) > 0:
                iid = "".join(temp_id)

            else:
                iid = None

            identifiers.append(iid)

        return identifiers

    def calculate_time_column(self, column, target_date, target_type= "single", measure = "years"):
        new_col = []
        if target_type== "multi":
            for i, dob in enumerate(column):
                if pd.notnull(dob):
                    complete_time = dateutil.relativedelta.relativedelta(target_date[i], dob)
                    if measure == "years":
                        time = complete_time.years
                    elif measure == "days":
                        time = complete_age.days
                    new_col.append(time)
                else:
                    new_col.append(None)

        elif target_type == "single":
            for dob in column:
                if pd.notnull(dob):
                    complete_time = dateutil.relativedelta.relativedelta(target_date, dob)
                    if measure == "years":
                        time = complete_time.years
                    elif measure == "days":
                        time = complete_time.days
                    new_col.append(time)
                else:
                    new_col.append(None)
        return new_col


    def get_unique(self, columns, write_to=None):

        if type(columns) != list:
            columns = [columns]

        big_list = []
        for index, row in self.df.iterrows():
            row_list = []
            for col in columns:
                row_list.append(row[col])
            row_tup = tuple(row_list)
            big_list.append(row_tup)

        unique = list(set(big_list))
        uni_df = pd.DataFrame(unique)

        if write_to:
            uni_df.to_csv(write_to, index=False)

        return uni_df

    def uppercase(self, column, df, alpha_col=False):
        '''
        Change all fields in a given column to uppercase. Changes in place.
        '''
        column = self.convert_column(self, column, alpha_col)

        df[column] = df[column].map(lambda x: x.upper() if type(x)==str and pd.notnull(x) else x)

    def fill_zeroes(self, column, df):
        '''
        Fills all nan in a given column with zeroes
        '''        
        df[column].fillna(value=0, inplace=True)

    def fill_nones(self, column, alpha_col=False):
        '''
        Fills all nan in a given column with None
        '''
        column = self.convert_column(self, column, alpha_col)
        
        self.df[column].fillna(value=None, inplace=True)

    def detect_similar(self, column, threshold = 80, alpha_col=False):
        column = self.convert_column(self, column, alpha_col)
        unique_instances = self.get_unique(column)
        primary_terms = set([])
        replace_dict = {}
        self.replace_dict[column] = {}

        for value in unique_instances:
            if type(value) == str:
                highest_ratio = 0
                best_match = None
                for key in primary_terms:
                    if value in primary_terms:
                        break
                    ratio = fuzz.ratio(key, value)
                    if ratio > threshold:
                        if ratio > highest_ratio:
                            highest_ratio = ratio
                            best_match = key
                if best_match:
                    replace_dict[value] = best_match
                else:
                    primary_terms.update({value})

            else:
                print(value)

        for value in unique_instances:
            if type(value) == str:
                highest_ratio = 0
                best_match = None
                for key in primary_terms:
                    if value in primary_terms:
                        break
                    ratio = fuzz.ratio(key, value)
                    if ratio > threshold:
                        if ratio > highest_ratio:
                            highest_ratio = ratio
                            best_match = key
                if best_match:
                    if best_match not in self.replace_dict[column]:
                        self.replace_dict[column].update({best_match : set([best_match])})
                    replace_dict[value] = best_match
                    self.replace_dict[column][best_match].update({value})
                else:
                    primary_terms.update({value})
            else:
                print(value)

        self.replace_dict[column] = replace_dict
        print("Number of Unique Labels", len(unique_instances))
        print("Number of Labels slotted for redisignation", len(self.replace_dict[column]))
        print("Labels without replacement", len(primary_terms))

    def build_replacement_dict(self, column, simuldict):

        replace_dict = {}

        for key, val_list in simuldict.items():
            for value in val_list:
                replace_dict[value] = key

        self.replace_dict[column] = replace_dict

    def replace_column(self, column, alpha_col=False):
        column = self.convert_column(self, column, alpha_col)

        self.original_df[column].replace(to_replace=self.replace_dict[column], inplace=True)

    def check_label_validity(self, column, alpha_col=False):
        column = self.convert_column(self, column, alpha_col)

        uniques = set(list(self.df[column].dropna()))

        not_accounted = {}

        for label in uniques:
            if label not in self.replace_dict[column]:
                not_accounted.update({label : {label}})

        return not_accounted

    def replace_all(self):
        self.original_df.replace(to_replace = self.replace_dict, inplace = True)

    def numbify_currency(self, column, df):
        
        converted_numbers = df[column].map(lambda x: float(x.strip("$").replace(",", "")) if type(x) == str and pd.notnull(x) else x)

        return converted_numbers

    def convert_dates(self, column, df):
        converted_dates = []

        for date in df[column]:

            if pd.notnull(date):
                new_date = dateparser.parse(str(date), settings={'PREFER_DATES_FROM': 'past'})
                
                if new_date.year > self.date.year:
                    new_year = new_date.year - 100
                    new_date = new_date.replace(year=new_year)

                converted_dates.append(new_date)

            else:
                converted_dates.append(date)

        return converted_dates

    #These field labels need to be changed for different spreadsheets
    def init_individuals(self):

        these_individuals = {}

        for index, row in self.df.iterrows():

            if pd.isnull(row[charge_disp]):
                charge_status = "UNKNOWN"

            else:

                charge_status = row[charge_disp]

            #check if headings are in original data

            if row[iid] in these_individuals:
                new_charge = {
                    charge_desc : row[charge_desc],
                    statute : row[statute],
                    bond : row[bond],
                    charge_class : row[charge_class],
                    charge_disp: charge_status,
                    disposition_type : self.case_disp[charge_status],
                    charge_rank : charge_rank_dict[row[charge_class]],
                    "Charge No Bond" : row[no_bond]
                    }

                these_individuals[row[iid]][charge_list].append(new_charge)
                these_individuals[row[iid]][indices].append(index)
                these_individuals[row[iid]][type_list].append(self.case_disp[charge_status])

            elif row[iid] not in these_individuals:
                these_individuals[row[iid]] = {
                    lastname : row[lastname],
                    firstname : row[firstname],
                    age : row[age],
                    gender : row[gender],
                    race : row[race],
                    admit: row[admit],
                    days_in : row[days_in],
                    city: row[city],
                    zipcode: row[zipcode],
                    totalbond : row[totalbond],
                    indices : [index],
                    type_list : [self.case_disp[charge_status]],
                    no_bond : False,
                    charge_list : [{
                        charge_desc : row[charge_desc],
                        statute : row[statute],
                        bond : row[bond],
                        disposition_type : self.case_disp[charge_status],
                        charge_class : row[charge_class],
                        charge_disp : charge_status,
                        charge_rank : charge_rank_dict[row[charge_class]],
                        "Charge No Bond" : row[no_bond]
                        }]
                    }


        self.individual_dict = these_individuals


            #add something about highest class charge, to detect bond / sentence descrepency
            #otherwise detect days confined high where bail is low

    def add_individals():
        pass

    def post_process_individuals(self):
        for iid, info in self.individual_dict.items():
            a = None
            b = "NO CHARGE"

            for charge in info[charge_list]:

                if charge["Charge No Bond"] == True:
                    info[no_bond] = True

                del charge["Charge No Bond"]

                if self.crimes_standardized:

                    if charge_rank_dict[charge[charge_class]] > charge_rank_dict[b]:
                        a = charge[charge_desc]
                        b = charge[charge_class]

                #look for case dispositions with secondary types and check type conditions

                if charge[charge_disp] in self.o.bonded_out:

                    if min(info[type_list]) == 7:
                        charge[disposition_type] = 2

                elif charge[charge_disp] in self.o.awaiting_trial:
                    
                    if info[no_bond]:
                        charge[disposition_type] = 3

            new_types = []

            for charge in info[charge_list]:
                new_types.append(charge[disposition_type])

            info[type_list] = new_types

            info[charge_disp] = typ_dict[min(new_types)]

            num_charge = len(info[charge_list])

            info.update({
                highest_charge : a,
                highest_charge_class : b,
                charge_rank : charge_rank_dict[b], 
                number_charges : num_charge
                })

    def detect_bond_discrepencies(self, remove=False, write_to=None):
        total_individuals = len(self.individual_dict)
        bond_conflicts_list = []
        conflict_indices = []
        for iid, info in self.individual_dict.items():
            calc_total = 0
            for charge in info["Charges"]:
                calc_total += charge["Bond"]
            if info["Total Bond"] != calc_total and info["Total Bond"] != 0:
                person = (info["Last Name"], info["First Name"], info["Last Admission Date"], info["Age"], iid)
                bond_conflicts_list.append(person)
                conflict_indices.extend(info["Indices"])

        if remove:
            for _, _, _, _, iid in bond_conflicts_list:
                del self.individual_dict[iid]
            print("%s of %s individuals removed from dictionary due to conflicts with bond totals" % (str(len(bond_conflicts_list)), total_individuals))

        else:
            print("%s of %s conflicting records" % (str(len(bond_conflicts_list)), total_individuals))

        # make condition for if deletions or rearrangrments have been made in log
        if write_to:
            conflicts_list = []
            for index in conflict_indices:
                conflicts_list.append(self.original_df.iloc[index]) #insure index does not change before this point
            conflicts_df = pd.DataFrame(conflicts_list)
            conflicts_df.to_csv(write_to, index=False)

        return bond_conflicts_list

    def find_stay_vs_bond(self, bond_above=0, bond_below=1000, days_above=0, days_below=float("inf"),  write_to=None, original_file=False):
        people = {}
        count = 0
        indices = []
        for iid, info in self.individual_dict.items():
            if info["Total Bond"] <= bond_below and info["Total Bond"] > bond_above \
            and info["Days in Custody from Last Admission"] > days_above \
            and info["Days in Custody from Last Admission"] <= days_below \
            and info["Total Bond"] != 0:
                count += 1
                people[iid] = info
                indices.extend(info["Indices"])

        if write_to:
            if original_file:
                data = self.original_df
            else:
                data = self.original_df
            row_list = []
            for index in indices:
                row_list.append(data.iloc[index]) #insure index does not change before this point
            mini_df = pd.DataFrame(row_list)
            mini_df.to_csv(write_to, index=False)

        return count, people

    def find_stay_vs_class(self, charge_above="NO CHARGE", charge_below=False, days_above=0, days_below=float("inf"),  write_to=None, original_file=False):
        people = {}
        count = 0
        indices = []

        min_charge = charge_rank_dict[charge_above]

        if not charge_below:
            max_charge = float("inf")
        else:
            max_charge = charge_rank_dict[charge_below]


        for iid, info in self.individual_dict.items():
            if info["Charge Rank"] <= max_charge and info["Charge Rank"] > min_charge \
            and info["Days in Custody from Last Admission"] > days_above \
            and info["Days in Custody from Last Admission"] <= days_below:
                count += 1
                people[iid] = info
                indices.extend(info["Indices"])

        if write_to:
            if original_file:
                data = self.original_df
            else:
                data = self.original_df
            row_list = []
            for index in indices:
                row_list.append(data.iloc[index]) #insure index does not change before this point
            mini_df = pd.DataFrame(row_list)
            mini_df.to_csv(write_to, index=False)

        return count, people

    def set_replacedict(self, column, outside_dict):
        self.replace_dict[column] = outside_dict

    def categorize(self, column, name, cat_dict, alpha_col=False):
        '''
        Takes dictionary form: {category_name : [fields, to, which, category, should,
        be, applied], category2_name : [etc]}

        Adds column to dataframe with header of "name" with categorical data
        '''
        column = self.convert_column(self, column, alpha_col)

        replace_dict = {}

        for key, val_list in cat_dict.items():
            for value in val_list:
                replace_dict[value] = key

        self.cat_dict[name] = replace_dict

        new_col = []

        for index, row in self.original_df.iterrows():
            if row[column] in self.cat_dict[name]:
                new_col.append(self.cat_dict[name][row[column]])
            else:
                new_col.append(row[column])

        return new_col

    def charge_tuples(self):
        new_col = []
        for i, row in self.original_df:
            crime_tuple = (row[self.o.charge_desc], row[self.o.statute])
            new_col.append(crime_tuple)
        return new_col

    def read_and_standardize_crimes(self, standardized_charges,
        orig_statute = "Original Statute", orig_desc = "Original Description",
        corr_statute = "Corrected Statute", corr_desc = "Standardized Description",
        write_to = None):

        crimes_df = pd.read_csv(standardized_charges, header=0) 
        self.crime_dict = {}
        self.crime_desc_dict = {}
        self.crime_class_dict = {}
        self.statute_dict = {}
        for index, row in crimes_df.iterrows():
            origin_tuple = (row[orig_desc], row[orig_statute])

            if row[corr_statute] not in self.crime_dict:

                self.crime_dict[row[corr_statute]] = \
                {corr_desc : row[corr_desc], \
                charge_class : row[charge_class], \
                corr_statute: row[corr_statute]}

            if row[orig_desc] not in self.crime_dict:

                self.crime_dict[row[orig_desc]] = \
                {corr_desc : row[corr_desc], \
                charge_class : row[charge_class], \
                corr_statute: row[corr_statute]}

            if origin_tuple not in self.crime_dict:

                self.crime_dict[origin_tuple] = \
                {corr_desc : row[corr_desc], \
                charge_class : row[charge_class], \
                corr_statute: row[corr_statute]}

            if row[orig_statute] not in self.crime_dict:

                self.crime_dict[row[orig_statute]] = \
                {corr_desc : row[corr_desc], \
                charge_class : row[charge_class], \
                corr_statute: row[corr_statute]}

        #check if statute is in corrective dictionary
        #then check if it is in standardized list

        statutes = []
        classes = []
        descriptions = []
        unresolved = []

        all_valid = True

        for i, row in self.original_df.iterrows():
            if (row[self.o.charge_desc], row[self.o.statute]) in self.crime_dict:
                determiner = (row[self.o.charge_desc], row[self.o.statute])
                statutes.append(self.crime_dict[determiner][corr_statute])
                classes.append(self.crime_dict[determiner][charge_class])
                descriptions.append(self.crime_dict[determiner][corr_desc])

            elif row[self.o.statute] in self.crime_dict:
                determiner = row[self.o.statute]
                statutes.append(self.crime_dict[determiner][corr_statute])
                classes.append(self.crime_dict[determiner][charge_class])
                descriptions.append(self.crime_dict[determiner][corr_desc])

            elif row[self.o.charge_desc] in self.crime_dict:
                determiner = row[self.o.charge_desc]
                statutes.append(self.crime_dict[determiner][corr_statute])
                classes.append(self.crime_dict[determiner][charge_class])
                descriptions.append(self.crime_dict[determiner][corr_desc])

            else:
                all_valid = False
                statutes.append(None)
                classes.append(None)
                descriptions.append(None)
                unresolved.append(row)

        self.df[statute] = statutes
        self.df[charge_class] = classes
        self.df[charge_desc] = descriptions

        if all_valid:
            print("All rows successfully processed")

        else:
            print("Not all rows could be processed")

        if write_to and not all_valid:
            ambi_rows = pd.DataFrame(unresolved)
            ambi_rows.to_csc(write_to)
            print("Unresolved rows written to " + write_to)


    def make_subset(self, i_list):
        '''
        Take a list of indices referring to indices in self.df and return a new DirtyData object with the data for these indices only
        '''
        row_list = []
        for i in i_list:
            row_list.append(self.orignal_df.iloc[i])
        new_df = pd.DataFrame(row_list)
        subset = DirtyData(new_df, self.date.month, self.date.day, self.date.year, source_type = "df")
        subset.init_individuals()
        subset.post_process_individuals()
        return subset

    def count_by_highest_charge(self):
        
        self.FM = 0
        self.FX = 0
        self.F1 = 0
        self.F2 = 0
        self.F3 = 0
        self.F4 = 0
        self.MA = 0
        self.MB = 0
        self.MC = 0

        for iid, info in self.individual_dict.items():
            if info["Highest Level Charge Class"] == "FM":
                self.FM += 1
            if info["Highest Level Charge Class"] == "FX":
                self.FX += 1
            if info["Highest Level Charge Class"] == "F1":
                self.F1 += 1
            if info["Highest Level Charge Class"] == "F2":
                self.F2 += 1
            if info["Highest Level Charge Class"] == "F3":
                self.F3 += 1
            if info["Highest Level Charge Class"] == "F4":
                self.F4 += 1
            if info["Highest Level Charge Class"] == "MA":
                self.MA += 1
            if info["Highest Level Charge Class"] == "MB":
                self.MB += 1
            if info["Highest Level Charge Class"] == "MC":
                self.MC += 1

    @staticmethod
    def convert_column(self, column, alpha_col):
        if alpha_col:
            numb = self.column_to_number(column)
            new_col = self.original_df.columns[numb]
        elif type(column) == int:
            new_col = self.original_df.columns[column]
        else:
            return column
        return new_col

    @staticmethod
    def str_to_date(self, datestring):
        dt = datestring.split("/")
        month = int(dt[0])
        day = int(dt[1])
        year = int(dt[2])

        if year >self.two_dig_year and year<1000:
            year += self.current_cent-100

        elif year <= self.two_dig_year:
            year += self.current_cent

        return datetime.date(year, month, day)

    @staticmethod
    def column_to_number(self, alpha_column):
        '''
        Takes column labels as they are in excel (i.e., "A", "AB", etc.) and translates 
        them to the corresponding number
        '''
        column_label = list(alpha_column)

        num = 0

        for letter in alpha_column:
            if letter in string.ascii_letters:
                num = num * 26 + (ord(letter.upper()) - ord("A"))+1

        return num-1
