#!/usr/bin/env python3
"""
Placeholder TensorFlow Model Generator for Fraud Scoring Pipeline.
Generates and serializes a Keras neural network to .h5 format.
Features: [amount, txn_count_15m, staleness_seconds] -> Sigmoid [fraud_probability]
"""

import os
import sys

MODEL_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(MODEL_DIR, "fraud_model.h5")


def generate_keras_h5_model():
    """Build and save a real Keras model in .h5 format if TensorFlow is present."""
    try:
        import tensorflow as tf
        from tensorflow import keras
        from tensorflow.keras import layers

        print(f"TensorFlow {tf.__version__} detected. Compiling placeholder neural net...")
        model = keras.Sequential([
            layers.Input(shape=(3,), name="transaction_features"),
            layers.Dense(16, activation="relu", name="dense_layer_1"),
            layers.Dense(8, activation="relu", name="dense_layer_2"),
            layers.Dense(1, activation="sigmoid", name="fraud_probability_output")
        ])

        model.compile(
            optimizer="adam",
            loss="binary_crossentropy",
            metrics=["accuracy"]
        )

        model.save(MODEL_PATH)
        print(f"Successfully generated and saved Keras model to: {MODEL_PATH}")
        return True

    except ImportError:
        print("TensorFlow not installed in current environment. Generating portable placeholder artifact...")
        # Create an HDF5-compatible or structured weight container if h5py is installed
        try:
            import h5py
            import json
            import numpy as np

            with h5py.File(MODEL_PATH, "w") as f:
                f.attrs["model_config"] = json.dumps({
                    "class_name": "Sequential",
                    "config": {
                        "name": "fraud_placeholder_model",
                        "layers": [
                            {"class_name": "InputLayer", "config": {"batch_input_shape": [None, 3]}},
                            {"class_name": "Dense", "config": {"units": 16, "activation": "relu"}},
                            {"class_name": "Dense", "config": {"units": 8, "activation": "relu"}},
                            {"class_name": "Dense", "config": {"units": 1, "activation": "sigmoid"}}
                        ]
                    }
                })
                f.attrs["keras_version"] = "2.15.0"
                f.attrs["backend"] = "tensorflow"
                f.create_group("model_weights")
            print(f"Generated HDF5 container at: {MODEL_PATH}")
            return True
        except Exception as e:
            # Fallback binary dummy
            with open(MODEL_PATH, "wb") as f:
                f.write(b"HDF5_FRAUD_MODEL_PLACEHOLDER_V1.0.0")
            print(f"Generated placeholder model binary file at: {MODEL_PATH} ({e})")
            return True


if __name__ == "__main__":
    generate_keras_h5_model()
