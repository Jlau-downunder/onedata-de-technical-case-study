<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->


<!-- PROJECT LOGO -->
<br />
<div align="center">
  
  <h3 align="center">Airflow</h3>
</div>


<!-- ABOUT THE PROJECT -->
## About The Project
This is a one data technical case study fast run development run through. The absolute bare minimum was completed based on what was instructed in this technical
take hom case study.

Please refer to this repo as a baseline of what could be expected in the in-person interview walk through of the candidates approach to the case study. 


<!-- GETTING STARTED -->
## Getting Started
### Prerequisites
1. Register a free snowflake trial account **This sql and Python is factored for snowflake connection and querying.**
2. Pull down this repository to your local machine.
3. Access ftp2.gov.bom.au via ftp client filezilla (using other ftp clients may force username/pw required) to retrieve files as instructed in case study and load them into your local machine in this repo's directory $/data/mock_input_file_system. <br><br>
You can also access the files directly from your pipeline, but this will require some minor python code refactoring. 

### Airflow Installation
1. Build the image
   ```sh
   docker compose -f docker-compose.yml build
   ```
2. Run the containers
    ```sh
   docker compose -f docker-compose.yml up
   ```
<!-- SETUP -->
## Database setup
1.  Open database_schema.snowql under $/utils/executable_snowql, run all the DDL commands in the dare warehouse to setup your database and schemas. Make sure you have the right privileges to do so.
2.  Open environment_category_tables_sp.snowql under $/utils/executable_snowql, run all the DDL and stored proc commands in the dare warehouse to setup your tables. Make sure you have the right privileges to do so.

## Run the pipeline
1. Access airflow web server on port:8081
2. Manually run the dag. Wait a few mins, check the data reconciliation log in task.
3. Access snowflake and do your analysis! 
<!-- LICENSE -->
## Technical case study data analysis question results
![image](https://github.com/Jlau-downunder/onedata-de-technical-case-study/assets/134078268/f0f1b244-14b8-4351-8a3e-f16d89abf683)
![image](https://github.com/Jlau-downunder/onedata-de-technical-case-study/assets/134078268/2d37f8c2-c2bc-4852-a3a6-fef89bcd106b)

