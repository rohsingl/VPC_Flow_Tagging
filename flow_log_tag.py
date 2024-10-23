import csv
import os
import sys
from collections import defaultdict

tagMap = {}
tagMap["Untagged"]=0
local_lookup = defaultdict(list)
port_to_protocol_mapping={}

def create_local_map():
     try:
        with open("port_to_protocol_mapping.csv", 'r') as file:
            port_to_protocol_mapping_obj = csv.DictReader(file)
            for row in port_to_protocol_mapping_obj:
                port_to_protocol_mapping[row['port']] = row['protocol']
     except FileNotFoundError:
        print("File: port_to_protocol_mapping.csv not found in current directory.")
        sys.exit(1)

     try:
        with open("lookup_file.csv", 'r') as file:
            local_lookup_obj = csv.DictReader(file)
            for row in local_lookup_obj:
                local_lookup[(row['dstport'])+':'+row['protocol']].append(row['tag'])
                local_lookup[(row['dstport'])+':'+row['protocol']].append(0)
     except FileNotFoundError:
        print("File: lookup_file.csv not found.")
        sys.exit(1)

def split_input_file(input_file, lines_per_file=3000):
        small_filename_list=[]
        small_filename = None
        i=0
        try:
            with open(input_file) as file:
                for lineno, line in enumerate(file):
                    if lineno % lines_per_file == 0:
                        i +=1
                        small_filename = 'small_file_{}.txt'.format(i)
                        small_filename_list.append(small_filename)
                    smallfile = open(small_filename, "a")
                    smallfile.write(line)
                smallfile.close()
            return small_filename_list
        except FileNotFoundError:
            print("File:{} not found in current directory".format(input_file))
            sys.exit(1)


# 1.Consider incoming packet for tagging if version is 2
# 2.Check for dstport:protocol key in local_lookup table
# 3.If found then increment the corresponding tag count otherwise increment "untagged"

def tag_match(batch_file_names):
    for files in batch_file_names:
        readLines = open(files, 'r')
        try:
            for line in readLines:
                line = line.lstrip()
                words = line.split(' ')
                if len(line) >=7:
                    version,dstport,protocol_no = int(words[0]), words[6],words[7]
                else:
                    continue

                if version ==2:
                    if protocol_no in port_to_protocol_mapping:
                        protocol_word = port_to_protocol_mapping[protocol_no].lower()
                        key=dstport+':'+protocol_word
                        if key in local_lookup:
                            #if local_lookup[key][0] not in tagMap:
                             #   tagMap[local_lookup[key][0]] =0
                            #tagMap[local_lookup[key][0]] +=1
                            tagMap[local_lookup[key][0]]=tagMap.get(local_lookup[key][0],0)+1
                            local_lookup[key][1] +=1
                        else:
                            tagMap["Untagged"] +=1
                    # Case where dstport matches but protocol not matched
                    else:
                        tagMap["Untagged"] +=1
            os.remove(files)
        except:
            print("Error in processing the sample_flow.txt file")
            for files in batch_file_names:
                os.remove(files)
            exit(1)


def generate_tag_count_output_file(fileName):
    tag_count_output_file = [["Tag","Count"]]
    # CSV file to generate tag,count output
    with open(fileName,"w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(tag_count_output_file[0])
        for key,value in tagMap.items():
            tag_count_output_file[0] = [key,value]
            writer.writerow(tag_count_output_file[0])

def generate_port_protocol_count_output_file(fileName):
    port_protocol_count_output_file = [["Port","Protocol","Count"]]
    # CSV file to generate port,protocol,tagcount output
    with open(fileName,"w") as file2:
        writer = csv.writer(file2)
        writer.writerow(port_protocol_count_output_file[0])
        for key,value in local_lookup.items():
            if value[0] in tagMap:
                val = key.split(':')
                port_protocol_count_output_file[0]=[val[0],val[1],value[1]]
                writer.writerow(port_protocol_count_output_file[0])

def main():
    batch_file_names=[]
    tag_count_output_file_name= "output_tag_count.txt"
    port_protocol_count_output_file_name = "output_port_protocol_count.txt"

    #Create Local Map for lookup_file and port_to_protocol_mapping
    create_local_map()

    #Split files into multiple files of 3k lines in each file to handle large file and memory
    batch_file_names= split_input_file("sample_flow.txt")

    #Main logic to count Tag
    tag_match(batch_file_names)

    #Generate output files
    print("Generating Count of matches for each tag output under output_tag_count.csv file")
    generate_tag_count_output_file(tag_count_output_file_name)

    print("Generating Count of matches for each port/protocol combination under output_port_protocol_count.csv file")
    generate_port_protocol_count_output_file(port_protocol_count_output_file_name)

if __name__ == '__main__':
    main()

