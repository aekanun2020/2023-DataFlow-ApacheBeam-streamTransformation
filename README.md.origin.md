# Apache Beam Pipeline for Real-Time Data Processing

This repository contains a Python script (`newCol-logEnable-revised-WORK.py`) that implements an Apache Beam pipeline. The pipeline processes real-time transaction 
data from a Google Pub/Sub topic and writes it to a Google BigQuery table.

## Table of Contents

- [Installation](#installation)
- [Pipeline Overview](#pipeline-overview)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

Follow these steps to set up the project:

1. Clone the repository:
    ```
    git clone https://github.com/aekanun2020/2023-DataFlow-ApacheBeam-streamTransformation.git
    ```

2. Install the required packages:
    ```
    pip install apache-beam[gcp]
    ```

## Pipeline Overview

The Beam pipeline processes real-time data in the following steps:

1. **Read from Pub/Sub:** The pipeline reads from a predefined Pub/Sub topic that streams transaction data in real-time.

2. **Cleanse Data:** The pipeline cleanses the data using a custom function called `cleanse_data`. This function does the following:
    - It strips white spaces from certain fields.
    - It tries to convert the `amount` field to a float. If conversion fails, it logs an error and sets `amount` to None.
    - It tries to convert the `tr_time_str` field to a datetime object, extracts the day of the week, and sets the `dayofweek` field accordingly. If conversion fails, 
it logs an error and sets `dayofweek` to None.

3. **Write to BigQuery:** The cleansed data is written to a predefined BigQuery table.

## Usage

To run the Beam pipeline, execute the Python script:

```bash
python newCol-logEnable-revised-WORK.py

