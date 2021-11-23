# Adding Azure service principal to Databricks from Windows command line
based on [microsoft documentation - scim](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/scim/scim-sp)


1. Create folder for all your scripts 
2. Within folder create file ".netrc" with:
```
machine <adb-address.azuredatabricks.net> login token password <your token>
```
Replace:\
<adb-address.azuredatabricks.net> - with Databricks address\
<your token> - with personal access token
  
 3. Create json file "create-service-principal.json" with 
  
 ```
  {
  "schemas": [ "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal" ],
  "applicationId": "<application-id>",
  "displayName": "<display-name>",
  "groups": [
    {
      "value": "<group-id>"
    }
  ],
  "entitlements": [
    {
      "value":"allow-cluster-create"
    }
  ]
}
```
<application-id> - service principal application id\
<group-id> - group id where SP will be assigned\
<display-name> - optional
  
4. Open CMD and CD into your folder (CD <path to created folder)
5. Execute following command
 ```  
 curl --netrc --netrc-file ".netrc" -X POST https://<adb-address.azuredatabricks.net>/api/2.0/preview/scim/v2/ServicePrincipals -H Content-type:application/scim+json --data @create-service-principal.json 
```
  
<adb-address.azuredatabricks.net> - replace with Databricks address
  

