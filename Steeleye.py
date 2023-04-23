import logging
import xml.etree.ElementTree as ET
import requests
import urllib.request
import zipfile
import csv
import boto3

# Set up logging
logging.basicConfig(filename='download_log.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Define URL and request XML content
url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
response = requests.get(url)

# Check for successful response
if response.status_code == 200:
    with open("file.xml", "wb") as f:
        # Write XML content to file
        f.write(response.content)
        logging.info("XML file downloaded successfully.")
else:
    logging.error("Error downloading XML file.")

try:
    # Parse the XML file
    tree = ET.parse('file.xml')
    root = tree.getroot()

    # Find the first download link whose file type is DLTINS
    download_link = None
    for doc in root.findall(".//doc"):
        file_type = doc.find("./str[@name='file_type']").text
        if file_type == 'DLTINS':
            download_link = doc.find("./str[@name='download_link']").text
            break

    # Print the download link
    print(download_link)

    # Log successful completion
    logging.info("Download link found successfully.")
except Exception as e:
    # Log error
    logging.error(f"Error finding download link: {e}")


try:
    # Download the zip file
    response = requests.get(download_link)

    # Save the zip file to disk
    with open("DLTINS.zip", "wb") as f:
        f.write(response.content)

    # Extract the xml file from the zip file
    with zipfile.ZipFile("DLTINS.zip", "r") as zip_ref:
        zip_ref.extractall()

    # Read the xml file
    with open("DLTINS_20210117_01of01.xml", "rb") as f:
        xml_data = f.read().decode("utf-8")

    # Print the xml data
    print(xml_data)

    # Log successful completion
    logging.info("Download and extraction successful.")
except Exception as e:
    # Log error
    logging.error(f"Error downloading or extracting file: {e}")


try:
    # Read the xml data from the file
    with open("DLTINS_20210117_01of01.xml", "rb") as f:
        xml_data = f.read().decode("utf-8")

    # Parse the xml data into an ElementTree object
    root1 = ET.fromstring(xml_data)

    # Create a list to store the extracted data
    dataa = []

    # Iterate over the FinInstrmGnlAttrbts elements and extract the required data
    for instrm in root1.iter("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts"):
        id = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id").text
        full_nm = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm").text
        clssfctn_tp = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp").text
        cmmdty_deriv_ind = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd").text
        ntnl_ccy = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy").text
        issr_elem = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr")
        issr = issr_elem.text if issr_elem is not None else "False"

        # Add the extracted data to the list
        dataa.append([id, full_nm, clssfctn_tp, cmmdty_deriv_ind, ntnl_ccy, issr])

    # Log successful completion
    logging.info("Data extraction successful.")
except Exception as e:
    # Log error
    logging.error(f"Error extracting data: {e}")
    dataa = []

try:
    # Write data to output.csv file
    logging.info("Starting the code to write data to output.csv file")
    with open("output.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"])
        writer.writerows(dataa)
    logging.info("Data written to output.csv file successfully")
except Exception as e:
    logging.error(f"Error occurred while writing data to output.csv file: {str(e)}")


try:
    # Upload output.csv file to S3 bucket
    logging.info("Starting the code to upload output.csv file to S3 bucket")
    bucket_name = 'amandeep12017226'
    file_name = 'output.csv'
    aws_access_key_id = ''
    aws_secret_access_key = ''
    region_name = 'ap-southeast-2'
    s3 = boto3.resource(
                service_name="s3",
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
    s3.Bucket(bucket_name).upload_file(Filename=file_name, Key='SteelEye_assignment.csv')
    logging.info("output.csv file uploaded to S3 bucket successfully")
except Exception as e:
    logging.error(f"Error occurred while uploading output.csv file to S3 bucket: {str(e)}")