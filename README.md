# Real-Time Data Processing with Apache Beam and Google DataFlow

This repository is designed to implement a real-time data processing pipeline. The pipeline processes real-time transaction data from a Google Pub/Sub topic and writes it to a Google BigQuery table. The repository contains two main Python scripts:

1. `newCol-logEnable-revised-WORK.py` - Implements an Apache Beam pipeline that processes real-time transaction data from a Google Pub/Sub topic and writes it to a Google BigQuery table.
2. `transactions_injector.py` - Generates simulated transaction data and publishes it to a Google Pub/Sub topic, simulating a real-time transaction stream.

## Table of Contents

- [Installation](#installation)
- [Pipeline Overview](#pipeline-overview)
- [Transaction Injector](#transaction-injector)
- [Google DataFlow](#google-dataflow)
- [Usage](#usage)
- [Video for Usage](https://video.aekanun.com/kQffsCPg)
- [Contributing](#contributing)
- [License](#license)

## Installation

Follow these steps to set up the project:

1. Clone the repository:
    ```bash
    git clone https://github.com/aekanun2020/2023-DataFlow-ApacheBeam-streamTransformation.git
    ```

2. Install the required packages:
    ```bash
    pip install apache-beam[gcp]
    ```

## Pipeline Overview

The Apache Beam pipeline processes real-time data in the following steps:

1. **Read from Pub/Sub:** The pipeline reads from a predefined Pub/Sub topic that streams transaction data in real-time.
2. **Cleanse Data:** The pipeline cleanses the data using a custom function called `cleanse_data`.
    - Strips white spaces from certain fields
    - Tries to convert the `amount` field to a float. If conversion fails, it logs an error and sets `amount` to None.
    - Tries to convert the `tr_time_str` field to a datetime object, extracts the day of the week, and sets the `dayofweek` field accordingly. If conversion fails, it logs an error and sets `dayofweek` to None.
3. **Write to BigQuery:** The cleansed data is written to a predefined BigQuery table.

## Transaction Injector

This Python script (`transactions_injector.py`) generates simulated transaction data and publishes it to a Google Pub/Sub topic. It is intended to be used alongside the Apache Beam pipeline to simulate a real-time transaction stream.

## Google DataFlow

The Apache Beam pipeline is orchestrated using Google DataFlow, ensuring a scalable and manageable real-time data processing pipeline.

### Overview

- `tr_time_str`: Transaction timestamp
- `first_name`: First name of the person initiating the transaction
- `last_name`: Last name of the person initiating the transaction
- `city`: City where the transaction occurred
- `state`: State where the transaction occurred
- `product`: Product involved in the transaction
- `amount`: Amount of the transaction in USD

### Usage

To start generating transactions and sending them to the Pub/Sub topic:

```bash
python transactions_injector.py
```
### Limitations and Considerations

- The script runs indefinitely and generates one transaction every 1 to 5 seconds.
- Ensure you have appropriate permissions to publish to the Pub/Sub topic.
- Note that this is a simulation and should not be used for production data.

## Usage

To run the Apache Beam pipeline, execute the Python script:

```bash
python newCol-logEnable-revised-WORK.py
```

## Video for Usage

For a video tutorial on how to use these scripts, you can [watch it here](https://video.aekanun.com/kQffsCPg).

## Contributing

Feel free to contribute to this project. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute.

## License

This project is licensed under the MIT License. See [LICENSE.md](LICENSE.md) for more details.
