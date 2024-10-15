import fiftyone as fo
import fiftyone.zoo as foz

dataset = foz.load_zoo_dataset("quickstart")
dataset.persistent = True
session = fo.launch_app(dataset,port=5151)
session.wait()