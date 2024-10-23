//Python 3.9.6

Assumptions:
Based upon information and understanding following assumptions has been made
1. Need to filter only version 2 flow logs
2. dstport and protocol value in input flow log will always be at index 6 and 6 respectively (index starts from 0)
3. If no dstport and protocol combination found in lookup table for incoming packet then it will be treated as untagged
4. If protocol is not found under “port_to_protocol_mapping.csv” file for incoming packet then it will be treated as untagged
5. Below 3 files should be present in the current directory from where you are running the code 
        1. sample_flow.txt: It will contain data for flow logs
        2. lookup_file.csv: The lookup table is defined in lookup_file.csv
        3. port_to_protocol_mapping.csv: Protocol mapping based on protocol_number

6. Output will be generated in below 2 csv files
    1. output_tag_count.csv
    2. output_port_protocol_count

7. For a dstport there could be multiple protocol packets terminating so that’s why taking key as (dstport:protocol)


Algorithm/Code:
1. Split the sample_flow.txt file into multiple small files of 3k line in each file to handle large files and memory overconsumption
2. Iterate over small files one by one to count the tag corresponds to (dstport,protocol)

How to Run:
   python3 flow_log_tag.py

