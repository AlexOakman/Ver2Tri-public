cd /path/to/Ver2Tri
source .ver2tri/bin/activate

# Basic run
python main.py

# Basic run without dashboard
python main.py --no-dashboard

# Queue inspection
python main.py --list --no-dashboard

# Process one file
python main.py --file sample_query --no-dashboard

# Retry in auto mode
python main.py --retry sample_query --no-dashboard

# Retry from a specific stage
python main.py --retry sample_query --retry-from pattern_guard --no-dashboard
python main.py --retry sample_query --retry-from trino_test --no-dashboard

# Reset metadata for one query
python main.py --reset sample_query --no-dashboard

# Reset metadata for all in-progress items
python main.py --reset-in-progress-states --no-dashboard

# Compile the DSPy module
python -m dspy_modules.compiler

# Force recompilation
python -m dspy_modules.compiler --force -n 30 -c 20 --bootstrapped-demos 6 --labeled-demos 6 --minibatch-size 22

# Useful checks
python -m compileall main.py config.py core dspy_modules tests
python -m pytest tests -q
