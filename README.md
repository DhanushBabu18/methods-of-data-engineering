# Economic Determinants of Gun Violence in the United States: Analysing Socioeconomic Trends and State-Level Disparities 
<img src="project\image.jpg" width="500" height="400">

## Project Overview
Gun violence is a critical issue in the United States, with significant social, economic, and political 
implications. Understanding the economic factors that contribute to its prevalence is essential for 
effective policymaking. This report aims to analyse the relationship between various economic 
indicators—such as GDP and GDP per capita—and the occurrence of gun violence across U.S. states. 

### Datasets
1. Gun Violence Data (U.S.): Contains information on gun violence incidents across the U.S., 
including location, date, number of killed and type of violence. The data covers a broad time 
frame and includes detailed event information. 
2. U.S. GDP by State (1997-2020): Provides GDP data for each U.S. state from 1997 to 2020, 
crucial for examining the economic landscape. 
3. GDP per Capita by State: Offers GDP per capita information by state, allowing us to assess 
economic disparities at the individual state level.

[**Project Data Report**](project/data-report.pdf): Document detailing data cleaning and pipeline procedures.

[**Project Analysis Report**](project/analysis-report.pdf): Final report containing data analysis and visualizations.

[**Project EDA**](project/EDA-report-analysis.ipynb): Notebook showcasing exploratory data analysis (EDA) for the project.

[**Presentation Slides**](project/slides.pdf)

[**Presenation Video Link**](project/presentation-video.md)

## Installation and Usage
Instructions for setting up the project environment and running the analysis scripts.

```bash
# Clone the repository
https://github.com/DhanushBabu18/methods-of-data-engineering.git

# Install dependencies
pip install -r requirements.txt

```

## Data Pipeline and Testing

### Data Pipeline [here](project/automated_datapipeline.py)
Our project includes an automated data pipeline designed for wildfire analysis:

1. **Data Fetching**: Automatically retrieves US Gun violence,GDP per state and GDP per capita datasets from specified sources.
2. **Data Transformation and Cleaning**: Applies necessary transformations and cleans the data to ensure accuracy and consistency.
3. **Data Loading**: Transformed data is loaded into structured formats suitable for analysis, ensuring integrity for further investigation

This pipeline ensures that our wildfire data is prepared and maintained for reliable analysis of trends and impacts.

### Test Script [here](project/automated_testing.py)
We have developed a rigorous test script to validate our wildfire data pipeline:

1. Tests include verification of data fetching accuracy.
2. Ensures proper data cleaning and transformation procedures are followed.
3. Validates data integrity and consistency throughout the pipeline.

### Automated Workflow [here](.github/workflows/test_runner.yml)
To maintain the reliability of our wildfire data pipeline, we have set up an automated workflow using GitHub Actions:

* **Continuous Integration Tests**: Automatically runs our test script upon every push to the main branch.Ensures any updates or modifications do not compromise the functionality and accuracy of the data pipeline.
  
This automated workflow guarantees a robust and error-free approach to analyzing wildfire trends and impacts, ensuring high-quality project outcomes.

## How to Run the Data Pipeline and Tests
Provide detailed instructions on how to execute the data pipeline and run the test scripts. Include any necessary commands or steps to set up the environment.

```bash
# command to run the data pipeline
python3 automated_datapipeline.py

# command to execute the test script
python3 automated_testing.py
```

## Contributing
We welcome contributions to this project! If you would like to contribute, please follow these steps:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature/YourFeature`).
3. Make your changes and commit them (`git commit -am 'Add some feature'`).
4. Push to the branch (`git push origin feature/YourFeature`).
5. Create a new Pull Request.

Please ensure your code is well-documented.

## Authors and Acknowledgment
This project was initiated and completed by Dhanush Hareesh Babu. 

## Special Thanks to Our Tutors:
I would like to extend my gratitude to our tutors for their guidance and support throughout this project.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.
