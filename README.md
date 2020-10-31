# YADskip
Project to skip youtube ads that are apart of the video based on captions.

Mongodb+python to pull in new videos and captions on set intervals

Other than a few cases of self labeled captions, captions will be labeled by a human.

TBD on the NLP model to use, but possibly BURT using active learning

If this gets passed the model phase and performs decent enough, the model can be exported to javascript and used in an extension without having to contact an external server. The extension would get the captions from the webpage and skip the parts of the video with captions labeled as ad. 