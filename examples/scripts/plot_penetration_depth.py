# -*- coding: utf-8 -*-
"""
Created on Wed Nov 08 12:04:38 2017

@author: u64125
"""

from mtpy.imaging import penetration_depth1d as pd

#edipath = r'C:\Git\mtpy\examples\data\edi_files'
edipath = r'examples\data\edi_files'
#elst = [op.join(edipath,f) for f in os.listdir(edipath) if f.endswith('.edi')]
elst = [pd.os.path.join(edipath,f) for f in pd.os.listdir(edipath) if f.endswith('.edi')]
print (elst)

#pplot=penetration_depth1d.Depth1D()
pplot=pd.Depth1D()
#p