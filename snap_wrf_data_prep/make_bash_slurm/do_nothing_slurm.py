# # # # 
# this is a simple script to do nothing so that we can use simple BASH if/else logic and make SLURM wait.
# # # # 

def do_nothing():
	pass

if __name__ == '__main__':
	import os

	_ = do_nothing()
	