# def get_objects_info(anvilobj, objects):
#     print("getting objects...")
#     for o in objects:
#         print("{}:\n\t{}".format(o, anvilobj.getinfo(o).raw))

# def list_dirs(anvilobj, folders):
#     print("listing directories...")
#     for d in folders:
#         print("{}:\n\t{}".format(d, anvilobj.listdir(d)))

# def run_all(anvilobj, files, folders):
#     get_objects_info(anvilobj, files+folders)
#     list_dirs(anvilobj, folders)