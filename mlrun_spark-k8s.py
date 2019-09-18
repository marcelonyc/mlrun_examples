#!/usr/bin/env python
# coding: utf-8

# In[2]:


import yaml
import json
from mlrun import get_or_create_ctx, run_start


context = get_or_create_ctx('sparkk8s')
sparkk8s_spec = context.get_param('sparkk8sspec', {})


# IGUAZIO environment
AUTH_SECRET = sparkk8s_spec.get('metadata.AUTH_SECRET',"shell-um81v5npue-qh8oc-v3io-auth")
FUSE_SECRET =  sparkk8s_spec.get('metadata.FUSE_SECRET',"shell-um81v5npue-qh8oc-v3io-fuse")

rundb=sparkk8s_spec.get('rundb','')
run_start(runtime=sparkk8s_spec, rundb=rundb, command='sparkk8s://')