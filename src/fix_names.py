import sys
import os
import csv

def translate_number(n_string):
    return(n_string.zfill(4))
    #return("%04d" % int(n_string))
    
if __name__=="__main__":

    # Set up our paths
    bad_case_list=sys.argv[1]
    output_case_list=sys.argv[2]
    
    prepend_string="17007_SCMP2DFA"

    # Get the bad list of cases
    bad_case_names=[]
    with open(bad_case_list,'r') as csvfile:
        case_list_reader=csv.reader(csvfile,delimiter=' ')
        for row in case_list_reader:
            bad_case_names.append(row[0])

    # Translate to correct case names
    good_case_names=[]
    for c in bad_case_names:
        good_name="{}{}".format(prepend_string,translate_number(c))
        print(good_name)
        good_case_names.append(good_name)

    # Write good case names to disk
    with open(output_case_list,'w') as f:
        for c in good_case_names:
            f.write(c+'\n')
