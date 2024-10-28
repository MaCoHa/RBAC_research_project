import create_wide_role_tree as wide
import create_deep_role_tree as deep
import create_balanced_role_tree as balanced

repetitions = 3
roles_to_create = 10_000
measurement_points = [500,1_000,2_000,4_000,8_000,10_000]

wide.main(repetitions,roles_to_create,measurement_points)
#deep.main(repetitions,roles_to_create,measurement_points)
#balanced.main(repetitions,roles_to_create,measurement_points)