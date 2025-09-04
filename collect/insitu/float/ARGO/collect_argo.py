import os
import sys

sys.path.append("../../../../ingest")
import vault_structure as vs


# june_2023 = "https://www.seanoe.org/data/00311/42182/data/103075.tar.gz"
# june_10_link = "https://www.seanoe.org/data/00311/42182/data/85023.tar.gz"
# july_10_link = "https://www.seanoe.org/data/00311/42182/data/86141.tar.gz"
# april_link = "https://www.seanoe.org/data/00311/42182/data/101760.tar.gz"
# july_2023 = "https://www.seanoe.org/data/00311/42182/data/103614.tar.gz"
# aug_2023 = "https://www.seanoe.org/data/00311/42182/data/104145.tar.gz"
# sep_2023 = 'https://www.seanoe.org/data/00311/42182/data/104707.tar.gz'
# oct_2030 = 'https://www.seanoe.org/data/00311/42182/data/105302.tar.gz'
# Nov2023 = 'https://www.seanoe.org/data/00311/42182/data/105924.tar.gz'
# Feb2024 = 'https://www.seanoe.org/data/00311/42182/data/108452.tar.gz'

def downloadArgo(newmonth, tar_url):
    """Download Argo tar file. Creates new vault tables based on newmonth stub
    Args:
        newmonth (string): Month and year of new data used as table suffix (ex. Sep2023)
        tar_url (string): URL pointing to tar download for newest data (ex. https://www.seanoe.org/data/00311/42182/data/104707.tar.gz)
    """
    tbl_list = [f'tblArgoCore_REP_{newmonth}',f'tblArgoBGC_REP_{newmonth}']
    for tbl in tbl_list:
        vs.leafStruc(vs.float_dir+tbl)
    base_folder = f'{vs.float_dir}{tbl}/raw/'
    output_dir = base_folder.replace(" ", "\\ ")      
    os.system(f"""wget --no-check-certificate {tar_url} -P {output_dir}""")


# https://github.com/simonscmap/cmapdata/blob/main/docs/data_ingestion/gallery_examples/outside_large_dataset_walkthrough.rst
# Argo GDAC:  https://www.seanoe.org/data/00311/42182/


newmonth = 'Jun2024'
tar_url = 'https://www.seanoe.org/data/00311/42182/data/110912.tar.gz'
    
downloadArgo(newmonth, tar_url)