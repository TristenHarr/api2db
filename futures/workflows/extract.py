# TODO: Extract Class
"""
The Extract class will support data transformations and will be subclassed to allow for data to be transformed in
more complex ways.

The Extract feature should have support to be placed ANYWHERE within the ApiForm and should follow a pub-sub type model

I.e. Extract allows for real-time summaries and features to be extracted from the data.

pre_process = [
    .
    .
    SubclassExtractFeaturePublishLatLong(  subpub_model="firestore",
                                          lam=lamdba x: [ x["data"]["lat"], x["data"]["long"] ],
                                          mode="summary",
                                          fmt_str="New data has been received with coordinates Lat: {} Long: {}"
                                        )
    .
    .
]

stores = [
    .
    .
    SubclassExtractFeatureSummarizeAverage( subpub_model="redis",
                                           lam=lambda x: .
                                                         .
                                                         .
                                           )

]
"""

class Extract(object):

    def __init__(self):
        pass

