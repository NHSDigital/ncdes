> Warning: This is the README for the publically accessible version of the NCDes package. If you are an analyst please don't use the below instructions to run the publication process.

<p>&nbsp;</p>

Repository owner: Primary Care Domain Analytical Team

Email: primarycare.domain@nhs.net

To contact us raise an issue on Github or via email and we will respond promptly.

<p>&nbsp;</p>

# NCDes package

This is a redacted version of the code used to produce the NCDes publicatons. The publications can be found here:

https://digital.nhs.uk/data-and-information/publications/statistical/mi-network-contract-des

<p>&nbsp;</p>

## Set up

1) Clone the repository to a location on your machine
2) Navigate to the cloned repository in a terminal (e.g. Anaconda Prompt, Windows Command Prompt etc)
3) Create and activate the correct environment by entering the below two sperate commands into your terminal:

```
conda env create --name ncdes --file environment.yml
```
```
conda activate ncdes
```

<p>&nbsp;</p>

## Creating a file structure 

To run this process locally you will need to create the below file structure on your machine and insert the provided files in the 'public_meta_data' folder as instructed in the 'Instructions for producing publication' steps.

> Warning: You will need to replace yy_yy in the 'NCD_yy_yy' folder and 'Data dictionary yy_yy' with the years relating to the correct quality service. E.g. NCD_yy_yy -> NCD_22_23

```
root
|
|---Input
|   |---Current
|   |   |---ncdes_synthetic_data.csv
|   |---Archive
|   |---Data dictionary yy_yy
|       |---indicator dictionary.csv
|       |---measure dictionary.csv
|
|---Output
|   |---Automated Checks
|   |---NCD_yy_yy
|
|---ePCN.xlsx
```

<p>&nbsp;</p>

## Instructions for publication production
After the above set up steps have been completed you can follow the below instructions to create the publication. Please note that you will not be able to run the code as this requires access to a private server. The data on the private server contains reference data that is used for mapping purposes. The reference tables used contain data from the [epraccur file](https://digital.nhs.uk/services/organisation-data-service/file-downloads/gp-and-gp-practice-related-data) and the [ONS code history database](https://www.ons.gov.uk/methodology/geography/geographicalproducts/namescodesandlookups/codehistorydatabasechd)

1) Move the 'config.json' from the 'public_meta_data' folder into the package at the same level as this 'README'.

2) In the config file edit the root directory value so that it matches the root of the directory that you set up. Make use of escape characters and end path with a double "\\\\" e.g. "\\\\\\\example\\\root\\\directory\\\\".

3) Download the epcn excel file from this [webpage](https://digital.nhs.uk/services/organisation-data-service/file-downloads/gp-and-gp-practice-related-data). Move it to the location specified in the above diagram. Copy the absolute path of this file and use it as the "epcn_path" in the config.json.

3) Move the 'ncdes_synthetic_data.csv' from the 'public_meta_data' folder into your '{root_directory}\Input\Current' folder.

4) Next you will need to move the indicator dictionary.csv and measure dictionary.csv into the Data dictionary yy_yy folder. To ensure you have the most up to date files download the latest data dictionary, this can be found on the relevant publication page. As an example the 22/23 service's data dictionary can be found [Here](https://files.digital.nhs.uk/1A/E5649B/NCDes_Data_Dictionary_22_23_v2.0.xlsx). You will then need to split the indicator and measure sheets into two individual csv files and name them 'indicator dictionary.csv' and 'measure dictionary.csv' respectively. An example of these files is given in the public_meta_data folder. Remember to move these files to the 'Data dictionary yy_yy' folder in your file tree. 

5) Run the 'create_publication.py' file by typing the below command into your terminal
    ```
    python -m ncdes.create_publication
    ```
The output of the job can then be found in the '{root_directory}\Output\NCD_yy_yy' folder.  

> WARNING: Please note that python uses the '\\' character as an escape character. To ensure your inserted paths work insert an additional '\\' each time it appears in your defined path. E.g.  'C:\Python25\Test scripts' becomes 'C:\\\Python25\\\Test scripts'  

<p>&nbsp;</p>

## Glossary of acronyms
There are a number of acronyms used in the text. They are set out in full and explained below:

 CQRS: Calculating Quality Reporting Service. The Calculating Quality Reporting Service (CQRS) is an approvals, reporting and payments calculation system for GP practices. More information on CQRS can be found [here](https://welcome.cqrs.nhs.uk/).

NCDes: Network Contract Directed Enhanced Services. This is explained in detail on this [page](https://digital.nhs.uk/data-and-information/publications/statistical/mi-network-contract-des/2022-23).

PCN: Primary care networks. Groups of GP practices working closely together - along with other healthcare staff and organisations - providing integrated services to the local population.

<p>&nbsp;</p>

## Licence
NCDes codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the Open Government 3.0 licence.