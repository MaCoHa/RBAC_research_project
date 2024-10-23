import create_wide_role_tree as wide
import create_deep_role_tree as deep
import create_balanced_role_tree as balanced

repetitions = 3
roles_to_create = 100
measurement_points = [1,10,100,1_000,10_000,100_000]

wide.main(repetitions,roles_to_create,measurement_points)
deep.main(repetitions,roles_to_create,measurement_points)
balanced.main(repetitions,roles_to_create,measurement_points)