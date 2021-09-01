package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"github.com/streadway/amqp"
)

func rabbitAmqpConnectionTLS(uri, caCertFile, clientCertFile, clientKeyFile string) (*amqp.Connection, error) {
	tlsConfig, err := generateRabbitTLSConfig(caCertFile, clientCertFile, clientKeyFile)
	if err != nil {
		return nil, err
	}

	return amqp.DialTLS(uri, tlsConfig)
}

func generateRabbitTLSConfig(caCertFile, clientCertFile, clientKeyFile string) (*tls.Config, error) {
	caCertFileBytes, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caCertFileBytes)

	customVerify := func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		roots := x509.NewCertPool()
		for _, rawCert := range rawCerts {
			cert, _ := x509.ParseCertificate(rawCert)
			roots.AddCert(cert)
			opts := x509.VerifyOptions{
				Roots: roots,
			}
			_, err := cert.Verify(opts)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return generateTLSConfig(caCertFile, clientCertFile, clientKeyFile, customVerify)
}

type customVerifyFunc func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error

// GenerateTLSConfig is used for tls dialing in go applications that are version >=1.15
func generateTLSConfig(caCertFile string, clientCertFile string, clientKeyFile string, customVerifyFunc customVerifyFunc) (*tls.Config, error) {
	clientKeyPair, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return nil, err
	}

	caCertFileBytes, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caCertFileBytes)

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientKeyPair},
		RootCAs:            roots,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		// Legacy TLS Verification using the new VerifyConnection callback
		// important for go version 1.15+ as some certificates in environments
		// that cause the new standard lib verification to fail.
		// This isn't really needed if your SSL certificates don't have the Common Name issue.
		// For more information: https://github.com/golang/go/issues/39568
		VerifyConnection: func(cs tls.ConnectionState) error {
			commonName := cs.PeerCertificates[0].Subject.CommonName
			if commonName != cs.ServerName {
				return fmt.Errorf("invalid certificate name %q, expected %q", commonName, cs.ServerName)
			}
			opts := x509.VerifyOptions{
				Roots:         roots,
				Intermediates: x509.NewCertPool(),
			}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		},
	}
	if customVerifyFunc != nil {
		tlsConfig.VerifyPeerCertificate = customVerifyFunc
		tlsConfig.VerifyConnection = nil
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}
