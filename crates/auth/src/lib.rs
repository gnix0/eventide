use anyhow::{Result, anyhow, bail};
use eventide_types::{AuthenticatedPrincipal, SubjectKind};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct OidcSettings {
    pub issuer: String,
    pub audience: String,
    pub public_key_pem: String,
}

pub struct OidcTokenValidator {
    decoding_key: DecodingKey,
    issuer: String,
    validation: Validation,
}

impl OidcTokenValidator {
    pub fn new(settings: OidcSettings) -> Result<Self> {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[settings.issuer.as_str()]);
        validation.set_audience(&[settings.audience.as_str()]);

        let decoding_key = DecodingKey::from_rsa_pem(settings.public_key_pem.as_bytes())
            .map_err(|error| anyhow!("invalid OIDC public key PEM: {error}"))?;

        Ok(Self {
            decoding_key,
            issuer: settings.issuer,
            validation,
        })
    }

    pub fn validate_bearer_token(&self, token: &str) -> Result<AuthenticatedPrincipal> {
        let token = token
            .strip_prefix("Bearer ")
            .or_else(|| token.strip_prefix("bearer "))
            .unwrap_or(token);

        if token.trim().is_empty() {
            bail!("bearer token must not be empty");
        }

        let data = decode::<KeycloakClaims>(token, &self.decoding_key, &self.validation)
            .map_err(|error| anyhow!("token validation failed: {error}"))?;

        let claims = data.claims;
        let audience = match claims.aud {
            Audience::Single(value) => vec![value],
            Audience::Multiple(values) => values,
        };

        let subject_kind = claims
            .preferred_username
            .as_deref()
            .and_then(|username| username.strip_prefix("service-account-"))
            .map_or(SubjectKind::User, |_| SubjectKind::ServiceAccount);

        Ok(AuthenticatedPrincipal {
            subject_kind,
            subject_id: claims.sub,
            preferred_username: claims.preferred_username,
            issuer: self.issuer.clone(),
            audience,
            realm_roles: claims
                .realm_access
                .map_or_else(Vec::new, |access| access.roles),
        })
    }
}

#[derive(Debug, Deserialize)]
struct KeycloakClaims {
    sub: String,
    aud: Audience,
    #[allow(dead_code)]
    iss: String,
    #[allow(dead_code)]
    exp: u64,
    preferred_username: Option<String>,
    realm_access: Option<RealmAccess>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Audience {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct RealmAccess {
    roles: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::{OidcSettings, OidcTokenValidator};
    use eventide_types::SubjectKind;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde::Serialize;

    const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJETqse41HRBsc\n7cfcq3ak4oZWFCoZlcic525A3FfO4qW9BMtRO/iXiyCCHn8JhiL9y8j5JdVP2Q9Z\nIpfElcFd3/guS9w+5RqQGgCR+H56IVUyHZWtTJbKPcwWXQdNUX0rBFcsBzCRESJL\neelOEdHIjG7LRkx5l/FUvlqsyHDVJEQsHwegZ8b8C0fz0EgT2MMEdn10t6Ur1rXz\njMB/wvCg8vG8lvciXmedyo9xJ8oMOh0wUEgxziVDMMovmC+aJctcHUAYubwoGN8T\nyzcvnGqL7JSh36Pwy28iPzXZ2RLhAyJFU39vLaHdljwthUaupldlNyCfa6Ofy4qN\nctlUPlN1AgMBAAECggEAdESTQjQ70O8QIp1ZSkCYXeZjuhj081CK7jhhp/4ChK7J\nGlFQZMwiBze7d6K84TwAtfQGZhQ7km25E1kOm+3hIDCoKdVSKch/oL54f/BK6sKl\nqlIzQEAenho4DuKCm3I4yAw9gEc0DV70DuMTR0LEpYyXcNJY3KNBOTjN5EYQAR9s\n2MeurpgK2MdJlIuZaIbzSGd+diiz2E6vkmcufJLtmYUT/k/ddWvEtz+1DnO6bRHh\nxuuDMeJA/lGB/EYloSLtdyCF6sII6C6slJJtgfb0bPy7l8VtL5iDyz46IKyzdyzW\ntKAn394dm7MYR1RlUBEfqFUyNK7C+pVMVoTwCC2V4QKBgQD64syfiQ2oeUlLYDm4\nCcKSP3RnES02bcTyEDFSuGyyS1jldI4A8GXHJ/lG5EYgiYa1RUivge4lJrlNfjyf\ndV230xgKms7+JiXqag1FI+3mqjAgg4mYiNjaao8N8O3/PD59wMPeWYImsWXNyeHS\n55rUKiHERtCcvdzKl4u35ZtTqQKBgQDNKnX2bVqOJ4WSqCgHRhOm386ugPHfy+8j\nm6cicmUR46ND6ggBB03bCnEG9OtGisxTo/TuYVRu3WP4KjoJs2LD5fwdwJqpgtHl\nyVsk45Y1Hfo+7M6lAuR8rzCi6kHHNb0HyBmZjysHWZsn79ZM+sQnLpgaYgQGRbKV\nDZWlbw7g7QKBgQCl1u+98UGXAP1jFutwbPsx40IVszP4y5ypCe0gqgon3UiY/G+1\nzTLp79GGe/SjI2VpQ7AlW7TI2A0bXXvDSDi3/5Dfya9ULnFXv9yfvH1QwWToySpW\nKvd1gYSoiX84/WCtjZOr0e0HmLIb0vw0hqZA4szJSqoxQgvF22EfIWaIaQKBgQCf\n34+OmMYw8fEvSCPxDxVvOwW2i7pvV14hFEDYIeZKW2W1HWBhVMzBfFB5SE8yaCQy\npRfOzj9aKOCm2FjjiErVNpkQoi6jGtLvScnhZAt/lr2TXTrl8OwVkPrIaN0bG/AS\naUYxmBPCpXu3UjhfQiWqFq/mFyzlqlgvuCc9g95HPQKBgAscKP8mLxdKwOgX8yFW\nGcZ0izY/30012ajdHY+/QK5lsMoxTnn0skdS+spLxaS5ZEO4qvPVb8RAoCkWMMal\n2pOhmquJQVDPDLuZHdrIiKiDM20dy9sMfHygWcZjQ4WSxf/J7T9canLZIXFhHAZT\n3wc9h4G8BBCtWN2TN/LsGZdB\n-----END PRIVATE KEY-----\n";
    const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2\npOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXB\nXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHR\nyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8Lw\noPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xq\ni+yUod+j8MtvIj812dkS4QMiRVN/by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5T\ndQIDAQAB\n-----END PUBLIC KEY-----\n";

    #[test]
    fn validates_user_tokens_and_extracts_realm_roles() {
        let validator = validator();
        let token = token(TestClaims {
            sub: String::from("user-123"),
            aud: vec![String::from("eventide-api")],
            iss: String::from("http://localhost:8081/realms/eventide"),
            exp: 4_102_444_800,
            preferred_username: Some(String::from("alice")),
            realm_access: TestRealmAccess {
                roles: vec![String::from("tenant-admin")],
            },
        });

        let principal = validator
            .validate_bearer_token(&token)
            .expect("token should validate");

        assert_eq!(principal.subject_kind, SubjectKind::User);
        assert_eq!(principal.subject_id, "user-123");
        assert_eq!(principal.realm_roles, vec![String::from("tenant-admin")]);
    }

    #[test]
    fn detects_keycloak_service_account_tokens() {
        let validator = validator();
        let token = token(TestClaims {
            sub: String::from("service-account-subject"),
            aud: vec![String::from("eventide-api")],
            iss: String::from("http://localhost:8081/realms/eventide"),
            exp: 4_102_444_800,
            preferred_username: Some(String::from("service-account-control-plane")),
            realm_access: TestRealmAccess { roles: Vec::new() },
        });

        let principal = validator
            .validate_bearer_token(&token)
            .expect("token should validate");

        assert_eq!(principal.subject_kind, SubjectKind::ServiceAccount);
    }

    #[test]
    fn rejects_tokens_with_the_wrong_issuer() {
        let validator = validator();
        let token = token(TestClaims {
            sub: String::from("user-123"),
            aud: vec![String::from("eventide-api")],
            iss: String::from("http://localhost:8081/realms/other"),
            exp: 4_102_444_800,
            preferred_username: Some(String::from("alice")),
            realm_access: TestRealmAccess { roles: Vec::new() },
        });

        let error = validator
            .validate_bearer_token(&token)
            .expect_err("token should fail validation");

        assert!(error.to_string().contains("token validation failed"));
    }

    fn validator() -> OidcTokenValidator {
        OidcTokenValidator::new(OidcSettings {
            issuer: String::from("http://localhost:8081/realms/eventide"),
            audience: String::from("eventide-api"),
            public_key_pem: String::from(PUBLIC_KEY),
        })
        .expect("validator should build")
    }

    fn token(claims: TestClaims) -> String {
        encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(PRIVATE_KEY.as_bytes()).expect("private key should parse"),
        )
        .expect("token should encode")
    }

    #[derive(Serialize)]
    struct TestClaims {
        sub: String,
        aud: Vec<String>,
        iss: String,
        exp: u64,
        preferred_username: Option<String>,
        realm_access: TestRealmAccess,
    }

    #[derive(Serialize)]
    struct TestRealmAccess {
        roles: Vec<String>,
    }
}
