# BarometricAltitude

## Setting up the environment and running the example

### Using a virtual environment (uv/venv)

1. Create a virtual environment with Python 3.12:
	```bash
	uv venv --python=3.12 venv
	source venv/bin/activate
	```

2. Install the required dependencies:
	```bash
	uv pip install -r requirements.txt
	```

3. Run the code example:
	```bash
	python -m barometric_altitude.dwd_open_data
	```

### Alternative: Using conda and environment.yml

1. Create and activate the environment with conda:
	```bash
	conda env create -f environment.yml
	conda activate barometric_altitude
	```

2. Run the code example:
	```bash
	python -m barometric_altitude.dwd_open_data
	```