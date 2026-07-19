use std::fmt;

use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use run_anywhere_contracts::{DebugSessionId, DebugSessionMode, JobId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugTokenClaims {
    pub aud: String,
    pub jti: String,
    pub mode: DebugSessionMode,
    pub exp: i64,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DebugTokenError {
    #[error("debug-token key ID must not be blank or contain control characters")]
    InvalidKeyId,
    #[error("debug-token JTI must not be blank or contain control characters")]
    InvalidJti,
    #[error("debug-token expiry must be in the future")]
    InvalidExpiry,
    #[error("invalid Ed25519 PKCS#8 signing key: {0}")]
    InvalidSigningKey(String),
    #[error("failed to sign debug token: {0}")]
    Signing(String),
}

/// Reusable Ed25519/EdDSA token issuer. The private key is parsed and exercised
/// once at construction and is intentionally omitted from Debug output.
#[derive(Clone)]
pub struct DebugTokenIssuer {
    key: EncodingKey,
    kid: String,
}

impl fmt::Debug for DebugTokenIssuer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DebugTokenIssuer")
            .field("kid", &self.kid)
            .finish_non_exhaustive()
    }
}

impl DebugTokenIssuer {
    pub fn from_ed25519_pkcs8_pem(
        pem: &[u8],
        kid: impl Into<String>,
    ) -> Result<Self, DebugTokenError> {
        let kid = kid.into();
        validate_kid(&kid)?;
        let key = EncodingKey::from_ed_pem(pem)
            .map_err(|error| DebugTokenError::InvalidSigningKey(error.to_string()))?;
        let issuer = Self { key, kid };

        // `from_ed_pem` validates the PEM/PKCS#8 envelope. Signing a harmless
        // startup probe also makes ring validate the embedded Ed25519 key bytes,
        // rather than discovering a malformed key on the first user request.
        let probe = DebugTokenClaims {
            aud: "startup-validation".to_owned(),
            jti: "startup-validation".to_owned(),
            mode: DebugSessionMode::Viewer,
            exp: 1,
        };
        issuer
            .encode_claims(&probe)
            .map_err(|error| DebugTokenError::InvalidSigningKey(error.to_string()))?;
        Ok(issuer)
    }

    pub fn kid(&self) -> &str {
        &self.kid
    }

    pub fn mint(
        &self,
        job_id: &JobId,
        session_id: &DebugSessionId,
        jti: &str,
        mode: DebugSessionMode,
        expires_at: DateTime<Utc>,
    ) -> Result<String, DebugTokenError> {
        self.mint_at(job_id, session_id, jti, mode, expires_at, Utc::now())
    }

    pub fn mint_at(
        &self,
        job_id: &JobId,
        session_id: &DebugSessionId,
        jti: &str,
        mode: DebugSessionMode,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<String, DebugTokenError> {
        validate_jti(jti)?;
        if expires_at <= now {
            return Err(DebugTokenError::InvalidExpiry);
        }
        let claims = DebugTokenClaims {
            aud: format!("{job_id}:{session_id}"),
            jti: jti.to_owned(),
            mode,
            exp: expires_at.timestamp(),
        };
        self.encode_claims(&claims)
            .map_err(|error| DebugTokenError::Signing(error.to_string()))
    }

    fn encode_claims(
        &self,
        claims: &DebugTokenClaims,
    ) -> Result<String, jsonwebtoken::errors::Error> {
        let mut header = Header::new(Algorithm::EdDSA);
        header.typ = Some("JWT".to_owned());
        header.kid = Some(self.kid.clone());
        encode(&header, claims, &self.key)
    }
}

fn validate_kid(kid: &str) -> Result<(), DebugTokenError> {
    if kid.trim().is_empty() || kid.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(DebugTokenError::InvalidKeyId);
    }
    Ok(())
}

fn validate_jti(jti: &str) -> Result<(), DebugTokenError> {
    if jti.trim().is_empty() || jti.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(DebugTokenError::InvalidJti);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use jsonwebtoken::{DecodingKey, Validation, decode, decode_header};

    use super::*;

    const PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIGrD/e7uKYqSY4twDEsRfMMuLSrODf14dpTiTK6K1YI0
-----END PRIVATE KEY-----
"#;
    const PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEA2+Jj2UvNCvQiUPNYRgSi0cJSPiJI6Rs6D0UTeEpQVj8=
-----END PUBLIC KEY-----
"#;

    #[test]
    fn minted_token_is_eddsa_audience_bound_and_verifiable() {
        let issuer = DebugTokenIssuer::from_ed25519_pkcs8_pem(PRIVATE_KEY, "debug-v1").unwrap();
        let now = Utc::now();
        let expires_at = now + Duration::minutes(10);
        let job_id = JobId::new("job_demo").unwrap();
        let session_id = DebugSessionId::new("dbg_demo").unwrap();
        let token = issuer
            .mint_at(
                &job_id,
                &session_id,
                "jti_demo",
                DebugSessionMode::Controller,
                expires_at,
                now,
            )
            .unwrap();

        let header = decode_header(&token).unwrap();
        assert_eq!(header.alg, Algorithm::EdDSA);
        assert_eq!(header.typ.as_deref(), Some("JWT"));
        assert_eq!(header.kid.as_deref(), Some("debug-v1"));

        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_audience(&["job_demo:dbg_demo"]);
        let decoded = decode::<DebugTokenClaims>(
            &token,
            &DecodingKey::from_ed_pem(PUBLIC_KEY).unwrap(),
            &validation,
        )
        .unwrap();
        assert_eq!(decoded.claims.jti, "jti_demo");
        assert_eq!(decoded.claims.mode, DebugSessionMode::Controller);
        assert_eq!(decoded.claims.exp, expires_at.timestamp());
    }

    #[test]
    fn invalid_key_and_non_future_expiry_are_rejected() {
        assert!(DebugTokenIssuer::from_ed25519_pkcs8_pem(b"not pem", "debug-v1").is_err());
        let issuer = DebugTokenIssuer::from_ed25519_pkcs8_pem(PRIVATE_KEY, "debug-v1").unwrap();
        let now = Utc::now();
        assert_eq!(
            issuer.mint_at(
                &JobId::new("job_demo").unwrap(),
                &DebugSessionId::new("dbg_demo").unwrap(),
                "jti_demo",
                DebugSessionMode::Viewer,
                now,
                now,
            ),
            Err(DebugTokenError::InvalidExpiry)
        );
    }
}
