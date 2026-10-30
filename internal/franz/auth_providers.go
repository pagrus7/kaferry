package franz

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	krbclient "github.com/jcmturner/gokrb5/v8/client"
	krbconfig "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/sasl"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func (s oauthSettings) mechanism() (sasl.Mechanism, error) {
	if s.tokenFile != "" {
		if _, err := readBearerToken(s.tokenFile); err != nil {
			return nil, invalidProperty("sasl.oauthbearer.token.file", "cannot read a nonempty bearer token")
		}
		return safeMechanism{Mechanism: oauth.Oauth(func(context.Context) (oauth.Auth, error) {
			token, err := readBearerToken(s.tokenFile)
			return oauth.Auth{Token: token}, err
		})}, nil
	}
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	if s.ca != "" {
		data, err := os.ReadFile(s.ca)
		if err != nil {
			return nil, invalidProperty("sasl.oauthbearer.ssl.ca.location", "cannot read CA certificates")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return nil, invalidProperty("sasl.oauthbearer.ssl.ca.location", "contains no CA certificates")
		}
		transport.TLSClientConfig.RootCAs = roots
	}
	provider := &oauthCredentials{
		config: clientcredentials.Config{ClientID: s.clientID, ClientSecret: s.secret, TokenURL: s.endpoint, Scopes: strings.Fields(s.scope)},
		client: &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("oauth token endpoint redirects are unsupported")
		}},
	}
	return safeMechanism{Mechanism: oauth.Oauth(provider.authenticate), cleanup: transport.CloseIdleConnections}, nil
}

type oauthCredentials struct {
	config clientcredentials.Config
	client *http.Client
	mu     sync.Mutex
	token  *oauth2.Token
}

func (p *oauthCredentials) authenticate(ctx context.Context) (oauth.Auth, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return oauth.Auth{}, err
	}
	if !p.token.Valid() {
		token, err := p.config.Token(context.WithValue(ctx, oauth2.HTTPClient, p.client))
		if err != nil {
			return oauth.Auth{}, err
		}
		if !validBearerToken(token.AccessToken) || (token.TokenType != "" && !strings.EqualFold(token.TokenType, "Bearer")) {
			return oauth.Auth{}, errors.New("invalid bearer token response")
		}
		p.token = token
	}
	return oauth.Auth{Token: p.token.AccessToken}, nil
}

func readBearerToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if !validBearerToken(token) {
		return "", errors.New("invalid bearer token file")
	}
	return token, nil
}

func validBearerToken(token string) bool {
	return token != "" && strings.IndexFunc(token, func(r rune) bool { return r <= ' ' || r > '~' }) == -1
}

func awsMechanism(provider aws.CredentialsProvider) sasl.Mechanism {
	return safeMechanism{Mechanism: awssasl.ManagedStreamingIAM(func(ctx context.Context) (awssasl.Auth, error) {
		credentials, err := provider.Retrieve(ctx)
		return awssasl.Auth{AccessKey: credentials.AccessKeyID, SecretKey: credentials.SecretAccessKey, SessionToken: credentials.SessionToken}, err
	})}
}

func defaultAWSMechanism() sasl.Mechanism {
	var mu sync.Mutex
	var provider aws.CredentialsProvider
	return awsMechanism(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		mu.Lock()
		defer mu.Unlock()
		if provider == nil {
			// Discover the SDK credential chain on first authentication, using its
			// cancellation context. Keep its cache so temporary credentials renew.
			cfg, err := awsconfig.LoadDefaultConfig(ctx)
			if err != nil {
				return aws.Credentials{}, err
			}
			provider = cfg.Credentials
		}
		return provider.Retrieve(ctx)
	}))
}

func (s kerberosSettings) mechanism() (sasl.Mechanism, error) {
	config, err := krbconfig.Load(s.config)
	if err != nil {
		return nil, invalidProperty("sasl.kerberos.config", "cannot load Kerberos configuration")
	}
	keys, err := keytab.Load(s.keytab)
	if err != nil {
		return nil, invalidProperty("sasl.kerberos.keytab", "cannot load Kerberos keytab")
	}
	user, realm, _ := strings.Cut(s.principal, "@")
	client := krbclient.NewWithKeytab(user, realm, keys, config)
	if _, err := client.IsConfigured(); err != nil {
		client.Destroy()
		return nil, invalidProperty("sasl.kerberos.config", "Kerberos client configuration is incomplete")
	}
	return safeMechanism{Mechanism: (kerberos.Auth{Client: client, Service: s.service}).AsMechanismWithClose()}, nil
}
