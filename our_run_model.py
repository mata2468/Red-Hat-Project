import argparse
import itertools
import os
import subprocess

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--data', default='./data/prom-label-proxy.txt', type=str, required=False)
parser.add_argument('--windows', default=24, type=int, required=False)
parser.add_argument('--horizon', default=24, type=int, required=False)
parser.add_argument('--normalize', default=2, type=int, required=False)
parser.add_argument('--series-to-plot', default=0, type=str, required=False)
# parser.add_argument('--save-plot', default='./', type=str, required=True)
# parser.add_argument('--save', default=f'./results/model_{}' type=str, required=True)
args = parser.parse_args()

# Hyperparameters with their default values
# hyperparameters = {
#     'CNNFilters': [50, 100, 150, 200],
#     'CNNKernel': [4, 6, 8, 10],
#     'GRUUnits': [50, 100, 150, 200],
#     'SkipGRUUnits': [5, 10, 15, 20],
#     'skip': [12, 24, 36, 48],
#     'dropout': [0.1, 0.2, 0.3, 0.4],
#     'highway': [12, 24, 36, 48],
#     'lr': [0.001, 0.002, 0.003, 0.004],
# }

hyperparameters = {
    'CNNFilters': [50],
    'CNNKernel': [4],
    'GRUUnits': [50],
    'SkipGRUUnits': [5],
    'skip': [12],
    'dropout': [0.2],
    'highway': [12],
    'lr': [0.002],
}
args.data = './data/prom-label-proxy-joined.txt'
save = './results/custom'

# Generate all combinations of hyperparameter values
param_combinations = list(itertools.product(*hyperparameters.values()))

# save = './results'
# Construct and run the command for each combination
for i, params in enumerate(param_combinations):
    model_name = f'model_{params[0]}_{params[1]}_{params[2]}_{params[3]}_{params[4]}_{params[5]}_{params[6]}_{params[7]}'	
    command = (
        f"python main.py --data={args.data} --window={args.windows} "
        f"--normalize={args.normalize} --CNNFilters={params[0]} --CNNKernel={params[1]} --GRUUnits={params[2]} "
        f"--SkipGRUUnits={params[3]} --skip={params[4]} --dropout={params[5]} --highway={params[6]} --lr={params[7]} "
        f"--save={os.path.join(f'{save}/models', model_name)} --no-log --predict=all --plot --series-to-plot={args.series_to_plot} "
        f"--save-plot={os.path.join(f'{save}/graphs', model_name)}"
    )
    command = 'python main.py --data="data/prom-label-proxy.txt" --predict=all --plot --series-to-plot=0 --save=./saves/model_1_2_3'
    print(f"Running command {i + 1}: {command}")
    exit(0)
    os.system(command)

