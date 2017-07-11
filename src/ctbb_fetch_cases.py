import sys
import os

import csv
import platform

import logging

MEGABYTE=1000000
GIGABYTE=1000000000

def usage():
    print(
        "USAGE:\n"
        "python ctbb_fetch_cases.py /path/to/case_list.csv /path/to/output_dir/\n"
        "\n"
        "    Input CSV file should actually just be a line-separated list of internal IDs. \n"
        "       i.e.:\n"
        "          10070_SCMP2DFA0001 \n"
        "          10070_SCMP2DFA0002\n"
        "          10070_SCMP2DFA0003\n"
        "                  ...\n"
        "\n"
        "    Output directory is the path into which all raw data will be copied\n"
        "\n"
        "    Paths, ideally, are absolute however they may be relative as well.\n"
        "\n"
        "C John Hoffman 2017\n"
    )

# Example patient ID code scheme:
# 10070_SCMP2DFA0001
# |---|               Lab group code ?
#       ||            Exeriment code (here, "screening chest")
#         |-|         Location code (here, Med Plaza 200)
#            |-|      Scanner code (here, Definition AS 64)
#               |--|  Patient internal ID

if platform.system()=='Linux':
    archive_path="/archive"
elif platform.system=="Windows":
    archive_path=r"\\skynet\cvib\syoung"

def fetch_case(raw_filepath,id_string,output_dirpath):
    ### Retrieve case, copy to output directory and rename with the internal ID
    (a,f_ext)=os.path.splitext(raw_filepath)    
    output_filename="{}{}".format(id_string,f_ext)
    import shutil
    logging.info("Copying {} to {}".format(raw_filepath,os.path.join(output_dirpath,output_filename)))
    shutil.copy(raw_filepath,os.path.join(output_dirpath,output_filename))

def parse_internal_id(id_string):
    ### Translate the internal ID codes into directory paths

    # Parse the raw string into parts
    irb_id     = id_string[0:5]
    experiment = id_string[6:8]
    location   = id_string[8:11]
    scanner    = id_string[11:14]
    patient    = id_string[14:18]
    
    # Eventually we'll do this with a lookup table, but for now, we
    # just need to get it done:
    if experiment=="SC":
        experiment="Screening_chest"
    elif experiment=="CO":
        experiment="Obstructive_Quantitative_Chest_Airways"

    if location=="MP2":
        if scanner=="DFA":
            scanner_loc="AS (MP200)"
        elif scanner=="FRC":
            scanner_loc="Force (MP200)"
    elif location=="MHB":
        if scanner=="DFA":
            scanner_loc="AS40 (MHB)"
        
    patient=patient.lstrip('0')
    if len(patient)<2: # super kludge for our bad directory naming (to be fixed in the future)
        patient='0'+patient
        
    patient_dirpath=os.path.join(archive_path,experiment,scanner_loc,patient)
    return patient_dirpath

def find_raw_data(patient_dirpath):
    ### Locate the raw data file inside of the patient's study directory

    # Check for PTR files (guaranteed raw data)
    raw_filestring=''
    
    for root, dirs, files in os.walk(patient_dirpath):
        for f in files:
            if f.endswith(".ptr"):
                raw_filestring=os.path.join(root, f)

    # If no PTR files, look for large IMA files
    if not raw_filestring:
        for root, dirs, files in os.walk(patient_dirpath):
            for f in files:
                filesize=os.stat(os.path.join(root,f)).st_size                
                if f.endswith(".IMA") and filesize>(500*MEGABYTE):
                    raw_filestring=os.path.join(root, f)

    return raw_filestring

if __name__=="__main__":

    logging.basicConfig(level=logging.INFO)

    # Define some paths
    if len(sys.argv)<2:
        usage()
        sys.exit()

    csv_filepath=sys.argv[1]
    output_dirpath=sys.argv[2]

    # Load the list of desired cases (pass the file as the first argument)
    cases=[]
    with open(csv_filepath,'r') as csvfile:
        case_list_reader=csv.reader(csvfile,delimiter=' ')
        for row in case_list_reader:
            cases.append(row[0])

    # Iterate over all cases listed
    # If case has associated PTR file, copy to output directory
    # If no PTR file is found, print an "error" (need to come up with "robust" handling for IMA files)
    for case in cases:
        id_string=case
        path=parse_internal_id(id_string)
        ptr_filepath=find_raw_data(path)

        if ptr_filepath:
            fetch_case(ptr_filepath,id_string,output_dirpath)
        else:
            print("{}: PTR file not found.".format(id_string))


        

