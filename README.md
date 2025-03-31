# smart-store-justin
## Virtual Environment Setup Command
```shell  
.venv\Scripts\activate
```
## Git Commands  
```shell  
 git add .
 ```
 ```shell
 git commit -m "Update"
 ```
 ```shell
 git push
 ```
 
## Run a Python Script
```shell  
py scripts\script_name.py
```

## Install Packages From Requirements
```shell  
py -m pip install --upgrade -r requirements.txt
```

## data_prep and data_scrubber
Data prep should clean andn standardize all three data files. It uses almost all funtions in in the data scrubber. I could not get it to replace missing values. Something is deleting all data rows with missing info.