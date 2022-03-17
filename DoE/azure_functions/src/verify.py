#Project: T19
#Engineer: Joseph Samo
#Purpose:  This code verifies the output from the DoE data set example. This code assumes the user has downloaded the outputs to DoE/azure_examples/downloads
#Revision: v1.0 Original Release
##############################################################################
from requests_html import HTMLSession
import glob
import os
import shutil
import sys
import traceback
import tempfile
import urllib.request
import zipfile

#Globals
site_url = "https://data.openei.org/submissions/573"

#Take in the url names for the DoE data, splice off the url and take the file name.
#Using the url, pull the zip file into downloads Finally, unzip the file in downloads.
def pull_file(url):   
    file_name=os.path.basename(url)

    #Make a temporary directory for the data.
    path =  tempfile.gettempdir()
     #Pull all of the zips from the website and unzip them into downloads.
    destination = os.path.join(path, file_name)

    # Check whether the folder exists or not
    folder_exist = os.path.exists(path)
    file_exist = os.path.exists(destination)   
    if not folder_exist:
        #Create a new directory because it does not exist 
        os.makedirs(path)  
        print("The new directory, %s is created!" %( path))
    if file_exist:
        #Create a new directory because it does not exist 
        print("Egad! There was a zip in %s with the same name." %( path))
        delete_file(destination) 
    
    print("Putting file %s in %s." %(file_name, path))
    urllib.request.urlretrieve(url, destination)
    with zipfile.ZipFile(destination, 'r') as zip_ref:
        zip_ref.extractall(path)
        print("Extraction for: %s.zip complete!" %(file_name))
    delete_file(destination)
        
#Function for removing file from storage.
#filename: path of the file+ filename + extentsion.
def delete_file(filename):
    if os.path.isfile(filename):# If file exists, delete it 
        os.remove(filename)
        print("Removed file: %s."  %(filename))
    else:# Show an error
        print("Could not remove %s. File not found."  %(filename))

#Remove csvs and zips from the temp directory.
def clear_temp_dir():
    #Select the temporary directory for the data.
    path =  tempfile.gettempdir()
    current_directory = os.getcwd()
    os.chdir(path) 
    temp_zips = glob.glob("*.zip")
    temp_csvs = glob.glob("*.csv")

    temp_zip_paths = [os.path.join(path, names) for names in temp_zips]
    temp_csv_paths = [os.path.join(path, names) for names in temp_csvs]
    for file in temp_zip_paths:
        delete_file(file)
    for file in temp_csv_paths:
        delete_file(file)
    os.chdir(current_directory)  
    
#Check to see if minor file exists in major file. This is to verify that all .csvs exist in the output.
#minor_files: The collection of individual files, in the order they were copied into the major_file.
#major: The collective set as a file descriptor. This is done so we can cycle over the major outside this call.
#Return boolean, are they all the same? True/False
def compare_files(minor_urls, i, major_file):
    line_number = 0
    return_val = True
    with open(major_file, 'r') as major:
        major_line = major.readline()
        for minor_url in minor_urls:
            url= 'https://data.openei.org/' + minor_url
            #get the url from the internet
            url=url.replace(" ", "%20")
            pull_file(url)
            minor_files, _ = get_local_files()
            minor_file=minor_files[i][0]
            with open(minor_file, 'r') as minor:
                minor_line = minor.readline()#Throw away header
                minor_line = minor.readline()#Grab the first line of data
                while minor_line:
                    # write this function yourself
                    major_line = major.readline()
                    line_number = line_number + 1
                    if minor_line != major_line:
                        # do something like print the line number, file name
                        #delete_file(minor_file)
                        print("File %s mismatched at line: %d" %( minor_file, line_number))
                        return_val = False
                        break
                    minor_line = minor.readline()
            for file in minor_files:
                delete_file(file[0])
    print("All minor exists in major!")
    return return_val

def get_local_files():    
    #Select the temporary directory for the data.
    temp_path = tempfile.gettempdir()
    download_path = os.path.dirname(__file__)
    download_path = os.path.join(download_path, "..", "downloads")
    current_directory = os.getcwd()
    os.chdir(temp_path)

    AMI_Census = glob.glob("*AMI Census*.csv")
    AMI_Census_path = [os.path.join(temp_path, names) for names in AMI_Census]
    AMI_State  = glob.glob("*AMI State*.csv")
    AMI_State_path = [os.path.join(temp_path, names) for names in AMI_State]
    FPL_Census = glob.glob("*FPL Census*.csv")
    FPL_Census_path = [os.path.join(temp_path, names) for names in FPL_Census]
    FPL_State  = glob.glob("*FPL State*.csv")
    FPL_State_path = [os.path.join(temp_path, names) for names in FPL_State]
    SMI_Census = glob.glob("*SMI Census*.csv")
    SMI_Census_path = [os.path.join(temp_path, names) for names in SMI_Census]
    SMI_State  = glob.glob("*SMI State*.csv")
    SMI_State_path = [os.path.join(temp_path, names) for names in SMI_State]
     #Combine all files in the list
    infile_lists = [AMI_Census_path, AMI_State_path, FPL_Census_path, FPL_State_path, SMI_Census_path, SMI_State_path]
    os.chdir(current_directory)

    out_file_names=["AMI_Census.csv", "AMI_State.csv", "FPL_Census.csv", "FPL_State.csv", "SMI_Census.csv", "SMI_State.csv"]
    out_file_paths= [os.path.join(download_path, names) for names in out_file_names]
    return infile_lists, out_file_paths
   
def main():
    try:
        #Get the links from the DoE website.
        session = HTMLSession()
        r = session.get(site_url)
        links= r.html.links
        
        #Parse out the data for just LEAD-data files.
        clear_temp_dir()
        LEAD_data=[]
        substring = "LEAD-data"
        for link in links:
            if substring in link:
                LEAD_data.append(link)
        LEAD_data.sort()
        
        #Take all of the data and put it in this location
        ###This works, I'm just commenting it out because it'll take forever to download all 50 states.
        print('Pulling from the following %d urls: %s' %(len(LEAD_data), LEAD_data ))
        if len(LEAD_data) == 0:
            print("No files to process!")
            exit(0)
        
        working_dir = os.path.dirname(__file__)
        _, major_files = get_local_files()
        for i, major_file in enumerate(major_files):
            matching = compare_files(LEAD_data, i, major_file)
        
            if not matching:
                print("Error in %s" %(major_file))
                status_code = 1
                result = "Error in " + major_file
                break
            else:
                status_code = 200
                result = "Sucess!"
    except:
        result = traceback.format_exc()
        print('We errored, but managed to catch it! Error is: %s' %(result ))
        #print('We errored, but managed to catch it! Error is: %s', result )
        status_code = 400

    return 0#func.HttpResponse(result, status_code = status_code)

if __name__ == "__main__":
    sys.exit(main())