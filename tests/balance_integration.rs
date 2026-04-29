/// Test d'intégration : vérifie que get_usdc_balance() retourne un solde valide.
/// Nécessite un .env valide avec POLYMARKET_PRIVATE_KEY et POLYMARKET_FUNDER.
/// Lancer avec : cargo test balance -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn test_get_usdc_balance() {
    use rusty_poly_sniper::config::Config;
    use rusty_poly_sniper::polymarket::PolymarketClient;
    use std::sync::Arc;

    let config = Config::from_env().expect("Config invalide — vérifie ton .env");
    let client = Arc::new(PolymarketClient::new(config));

    let balance = client
        .get_usdc_balance()
        .await
        .expect("get_usdc_balance() a échoué");

    println!("\n✓ Solde USDC : {:.2} $", balance);
    assert!(balance >= 0.0, "Le solde ne peut pas être négatif");
}
