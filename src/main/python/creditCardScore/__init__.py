import pandas as pd
import statsmodels.api as sm


spector_data = sm.datasets.spector.load()
spector_data.exog = sm.add_constant(spector_data.exog)
logit_mod = sm.Logit(spector_data.endog, spector_data.exog)
logit_res = logit_mod.fit()
print(logit_res.summary())