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

## Creating a file structure 

Firstly, clone the repository to a location on your machine.

To run this process locally you will need to create the below file structure on your machine and insert the provided files in the 'public_meta_data' folder as instructed in the 'Instructions for producing publication' steps.

```
root
|
|---Input
|   |---Current
|   |   |---ncdes_synthetic_data.csv
|   |---Archive
|   |---CQRS_files
|       |---NCD_CQRS_Metadata_Mapping.csv
|       |---NCD_metadata.csv
|
|---Output
|   |---Automated Checks
|   |   |---Missing Indicators History.csv
|   |   |---Missing Measures History.csv
|   |   |---Unexpected Indicators History.csv
|   |   |---Unexpected Measures History.csv
|   |---NCD_yy_yy
|   |---Opt Out Tracking
|   |   |---Archive
|   |   |   |---NCD_Opt_Out_Tracking_month.xlsx
|   |   |---NCD_Opt_Out_Tracking.xlsx
|   |---CQRS_omitted_tracker
|   |   |---CQRS_omits.csv
| 
|---templates
|   |---CONTROL_FILE_NCDes.csv
|   |---template NCDES LDHC Ethnicity Recording MMM YYYY.xlsx
|
|---ePCN.xlsx
```

<p>&nbsp;</p>

## Instructions for publication production
After the above set up steps have been completed you can follow the below instructions to create the publication. Please note that you will not be able to run the code as this requires access to a private server. The data on the private server contains reference data that is used for mapping purposes. The reference tables used contain data from the [epraccur file](https://digital.nhs.uk/services/organisation-data-service/file-downloads/gp-and-gp-practice-related-data) and the [ONS code history database](https://www.ons.gov.uk/methodology/geography/geographicalproducts/namescodesandlookups/codehistorydatabasechd)

1) In the config file edit the root directory value so that it matches the root of the directory that you set up. Make use of escape characters and end path with a double "\\\\" e.g. "\\\\\\\example\\\root\\\directory\\\\".

2) Download the epcn excel file from this [webpage](https://digital.nhs.uk/services/organisation-data-service/file-downloads/gp-and-gp-practice-related-data). Move it to the location specified in the above diagram. Copy the absolute path of this file and use it as the "epcn_path" in the config.json.

3) Move the 'ncdes_synthetic_data.csv' from the 'public_meta_data' folder into your '{root_directory}\Input\Current' folder.

4) Next you will need to ensure that any files within the public metadata folder are populated appropriately and moved to the appropriate location as per the file structure above.

5) Double click "run_ncdes.bat" to run the job. The job will begin by downloading the relevant python packages into a virtual environment and then running the job. The output of the job can then be found in the '{root_directory}\Output\YY_YY' folder. This will open automatically once the job is complete. 

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