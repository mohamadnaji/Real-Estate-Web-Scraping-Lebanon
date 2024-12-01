from datetime import datetime

import pandas as pd
import os
import shutil
from datetime import datetime


# Step 2: Define the services map (reversed)
services_map = {
    # Security Features
    "Security": "Security Features",
    "Security service": "Security Features",
    "Video security": "Security Features",
    "Security door": "Security Features",
    "Alarm system": "Security Features",
    "Fire alarm system": "Security Features",
    "Videophone": "Security Features",
    "Electric gate": "Security Features",
    "Metal shutters": "Security Features",
    "Optical fiber": "Security Features",  # (if used for security systems)
    "Safe": "Security Features",

    # Heating and Cooling
    "Air-conditioning": "Heating and Cooling",
    "Central A/C & heating": "Heating and Cooling",
    "Central A/C": "Heating and Cooling",
    "Double flow ventilation": "Heating and Cooling",
    "Simple flow ventilation": "Heating and Cooling",
    "Electric awnings": "Heating and Cooling",
    "Electric shutters": "Heating and Cooling",
    "Sliding windows": "Heating and Cooling",
    "Aluminum window": "Heating and Cooling",
    "Double glazing": "Heating and Cooling",
    "Window shade": "Heating and Cooling",
    "Fireplace": "Heating and Cooling",

    # Fitness and Recreation
    "Fitness": "Fitness and Recreation",
    "Swimming pool": "Fitness and Recreation",
    "Shared Pool": "Fitness and Recreation",
    "Private Pool": "Fitness and Recreation",
    "Children's Pool": "Fitness and Recreation",  # Added from new services
    "Jacuzzi": "Fitness and Recreation",
    "Private Jacuzzi": "Fitness and Recreation",
    "Sauna": "Fitness and Recreation",
    "Tennis court": "Fitness and Recreation",
    "Playground": "Fitness and Recreation",
    "Boules court": "Fitness and Recreation",  # Added from new services
    "Bowling game": "Fitness and Recreation",
    "Spa": "Fitness and Recreation",
    "Shared Spa": "Fitness and Recreation",
    "Private Gym": "Fitness and Recreation",
    "Shared Gym": "Fitness and Recreation",
    "Playroom": "Fitness and Recreation",

    # Technology and Utilities
    "Lift": "Technology and Utilities",
    "Elevator": "Technology and Utilities",  # assuming it's the same as Lift
    "Engine generator": "Technology and Utilities",
    "Central vacuum system": "Technology and Utilities",
    "Internet": "Technology and Utilities",
    "Networked": "Technology and Utilities",
    "Solar panels": "Technology and Utilities",
    "Water softener": "Technology and Utilities",
    "24/7 Electricity": "Technology and Utilities",

    # Luxury and Convenience
    "Concierge": "Luxury and Convenience",
    "Caretaker": "Luxury and Convenience",
    "Caretaker house": "Luxury and Convenience",  # Added from new services
    "Reception 24/7": "Luxury and Convenience",
    "Maid Service": "Luxury and Convenience",
    "Maids Room": "Luxury and Convenience",
    "Business center": "Luxury and Convenience",
    "Maintenance": "Luxury and Convenience",
    "Lobby in Building": "Luxury and Convenience",
    "Kitchen Appliances": "Luxury and Convenience",
    "Built in Kitchen Appliances": "Luxury and Convenience",
    "Listed historic building": "Luxury and Convenience",
    "Accessible": "Luxury and Convenience",

    # Outdoor and Landscaping
    "Private Garden": "Outdoor and Landscaping",
    "Barbecue Area": "Outdoor and Landscaping",
    "Terrace": "Outdoor and Landscaping",
    "Balcony": "Outdoor and Landscaping",
    "Outdoor lighting": "Outdoor and Landscaping",
    "Covered Parking": "Outdoor and Landscaping",

    # Views
    "View of Landmark": "Views",
#    "City View": "Views",
    "View of Water": "Views",
    "Mountain View": "Views",
    "Sea view": "Views",

    # Storage and Space
    "Storage Room": "Storage and Space",
    "Storage room": "Storage and Space",  # Assuming this is the same as Storage Room
    "Built in Wardrobes": "Storage and Space",
    "Walk-in Closet": "Storage and Space",
    "Study": "Storage and Space",
    "Study Room": "Storage and Space",
    "Attic/ Loft": "Storage and Space",

    # Child-Friendly
    "Children's Play Area": "Child-Friendly",

    # Pet-Friendly
    "Pets Allowed": "Pet-Friendly",

    # Near Amenities
    "Near restaurants": "Near Amenities",
    "Near Public transportation": "Near Amenities",
}

def organize_categories(path):
    df = pd.read_csv(path)
    # Step 3: Add new columns to the DataFrame for each category counter and initialize to 0
    categories = set(services_map.values())  # Unique categories
    for category in categories:
        df[category] = 0

    # Step 4: Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Step 5: Initialize counters for the current row
        category_counts = {category: 0 for category in categories}
        print(index)

        # Step 6: Iterate over each service in the services_map
        for service, category in services_map.items():
            # Check if the service exists (value is 1)
            if row.get(service) is not None and row.get(service) in [1, 1.0, "1.0", "1.00000", "1"]:
                print(service)
                # Increment the count for the corresponding category
                category_counts[category] += 1

        # Step 7: Update the DataFrame with the category counts for the current row
        for category, count in category_counts.items():
            df.at[index, category] = count
        print('=====================================')


    # # Step 8: Remove the service columns from the DataFrame
    service_columns = list(services_map.keys())
    stripped_columns = df.columns.str.strip()
    common_elements = list(set(service_columns) & set(stripped_columns))
    df.drop(columns=common_elements, inplace=True)


    # Optionally, save the combined DataFrame to a new CSV file
    df.to_csv(path, index=False)


def move_csv_files(src_dir, dest_dir):
    # Append the current date to the destination directory name
    current_date = datetime.now().strftime("%Y-%m-%d")
    dest_dir_with_date = f"{dest_dir}_{current_date}"

    # Create the destination directory if it doesn't exist
    if not os.path.exists(dest_dir_with_date):
        os.makedirs(dest_dir_with_date)

    # Walk through the source directory
    for root, dirs, files in os.walk(src_dir):
        # Get the relative path to keep directory structure
        relative_path = os.path.relpath(root, src_dir)
        dest_subdir = os.path.join(dest_dir_with_date, relative_path)

        # Create subdirectories in the destination
        if not os.path.exists(dest_subdir):
            os.makedirs(dest_subdir)

        # Move only CSV files
        for file in files:
            if file.endswith(".csv"):
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_subdir, file)
                shutil.move(src_file, dest_file)  # Move the file


def read_files(directory):
    # List to hold DataFrames
    dataframes = []

    # Loop through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):  # Check if the file is a CSV file
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path)  # Read the CSV file
            dataframes.append(df)  # Append the DataFrame to the list

    combined = pd.concat(dataframes, axis=0, ignore_index=True)
    return combined


def combine_sources(data_type):
    script_dir = os.path.dirname(os.path.abspath(__file__))

    df1 = read_files(script_dir + '/dubizzle/' + data_type)
#    df2 = read_files(script_dir + '/ose/' + data_type)
    df3 = read_files(script_dir + '/pbm/' + data_type)
    df4 = read_files(script_dir + '/realEstate/' + data_type)
    # Concatenate the DataFrames vertically (along rows)
    combined_df = pd.concat([df1, df3, df4], axis=0, ignore_index=True)
    # Optionally, save the combined DataFrame to a new CSV file

    current_datetime = datetime.now().strftime('%Y-%m-%d')
    file_name = 'combined_' + data_type + '_' + current_datetime + '.csv'
    file_path = f'{script_dir}/combined/{data_type}/{file_name}'
    combined_df.to_csv(str(file_path), index=False)

    print('Organizing categories')
    organize_categories(str(file_path))
    print('Organized successfully')
    return combined_df


combined_sale = combine_sources('sale')
combined_rent = combine_sources('rent')

script_dir = os.path.dirname(os.path.abspath(__file__))

move_csv_files(script_dir + '/dubizzle', script_dir + '/old/old')
#move_csv_files(script_dir + '/ose', script_dir + '/old/old')
move_csv_files(script_dir + '/pbm', script_dir + '/old/old')
move_csv_files(script_dir + '/realEstate', script_dir + '/old/old')

