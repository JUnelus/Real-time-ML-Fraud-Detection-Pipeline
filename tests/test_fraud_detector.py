from src.data_generator.transaction_simulator import TransactionSimulator
from src.ml_model.fraud_detector import FraudDetector


def test_fraud_detector_trains_predicts_and_persists_model(tmp_path):
    simulator = TransactionSimulator()
    training_data = simulator.generate_batch(size=120, fraud_rate=0.15)

    model_path = tmp_path / "fraud_model_test.pkl"
    detector = FraudDetector(model_path=str(model_path))
    accuracy = detector.train(training_data)

    prediction = detector.predict_single(simulator.generate_fraud_transaction())
    detector.save_model()

    reloaded = FraudDetector(model_path=str(model_path))
    assert reloaded.load_model() is True

    assert accuracy >= 0
    assert prediction["risk_level"] in {"LOW", "MEDIUM", "HIGH"}
    assert isinstance(prediction["risk_factors"], list)
    assert model_path.exists()


