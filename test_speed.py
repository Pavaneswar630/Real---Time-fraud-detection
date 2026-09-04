import time 
import numpy as np 
import tensorflow as tf 
model = tf.keras.models.load_model('fraud_service/scoring/ml/fraud_model.keras') 
dummy_input = np.array([[150.0, 2.0, 10.0]], dtype=np.float32) 
_ = model(dummy_input, training=False) 
start = time.perf_counter() 
for _ in range(100): 
    _ = model(dummy_input, training=False) 
elapsed = time.perf_counter() - start 
print(f'Average inference time: {(elapsed / 100) * 1000:.2f}ms') 
