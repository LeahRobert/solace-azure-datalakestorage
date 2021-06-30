# solace-azure-datalakestorage
Consume from a Solace queue &amp; store/append data in Azure Data Lake Storage Gen2

⚠️ Keep in mind that this code has not been tested or coded to be PRODUCTION ready.

## Environment Setup
1. [Install Python 3.7](https://www.python.org/downloads/) (See installed version using `python3 -V`)   
    1.1 Note: If you are installing python for the first time on your machine then you can just use `python` instead of `python3` for the commands
1. [Optional] Install virtualenv `python3 -m pip install --user virtualenv`     
    1.1 Note: on a Linux machine, depending on the distribution you might need to `apt-get install python3-venv` instead
1. Clone this repository
1. [Optional] Setup python virtual environment `python3 -m venv venv`
1. [Optional] Activate virtual environment:     
    1.1 MacOS/Linux: `source venv/bin/activate`   
    1.2 Windows: `source venv/Scripts/activate`     
1. After activating the virtual environment, make sure you have the latest pip installed `pip install --upgrade pip`

## Install the Solace Python API and other dependencies
1. Install the API `pip install -r requirements.txt`

## Set Azure DataLake Credentials
Specify the Storage Account Name & Storage Account Key in the storeToADLS method

- storage_account_name = '<account-name'
- storage_account_key = '<account-key>'

## Run File
Execute the script as follows:

- `python solace_to_adls.py`

Note: This assumes you have a [local docker](https://solace.com/products/event-broker/software/getting-started/) broker running on localhost
It also assumes you have a queue named 'adls' on your broker

To pass non default parameters, do so via the environment variables   
- `SOLACE_HOST=<host_name> SOLACE_VPN=<vpn_name> SOLACE_USERNAME=<username> SOLACE_PASSWORD=<password> python <name_of_file>.py`

## Notes:
1. [Python Virtual environment](https://docs.python.org/3/tutorial/venv.html) is recommended to keep your project dependencies within the project scope and avoid polluting global python packages
1. Solace hostname, username, message vpn, and password are obtained from your Solace cloud account
1. Make sure you have the latest pip version installed prior installation

## Resources
- Solace Developer Portal is at [solace.dev](https://solace.dev)
- Official python documentation for Solace [https://docs.solace.com/Solace-PubSub-Messaging-APIs/Python-API/python-home.htm](https://docs.solace.com/Solace-PubSub-Messaging-APIs/Python-API/python-home.htm)
- Azure Documenation [https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python]
- Official python documentation for Azure DataLakeClient [https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-file-datalake/12.0.0b7/azure.storage.filedatalake.html]