# conda install scikit-learn
import scipy
import numpy as np
import itertools
from sklearn import linear_model
from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import StratifiedKFold

from getDependentFeatures import getDependentFeatures
from twitter.getFeatures import getTwitterFeatures
from spotify_data.machinelearning.getFeatures import getSpotifyFeatures
from discogs_data.machinelearning.getFeatures import getDiscogsFeatures
from spotify_data.songfeatures.fetchAudioData import getAudioData


def test_classifier(clf, X, Y):
    folds = StratifiedKFold(Y, 5)
    aucs = []
    accs = []
    for train, test in folds:
        # Sizes
        # print X[train].shape, Y[train].shape
        # print X[test].shape, len(prediction)

        clf.fit(X[train], Y[train])
        prediction = clf.predict(X[test])
        prediction_proba = clf.predict_proba(X[test])
        accs.append(accuracy_score(Y[test], prediction))
        aucs.append(roc_auc_score(Y[test], prediction_proba[:, 1]))
    print "Acc:\t", clf.__class__.__name__, "\t", np.mean(accs)
    #print "AUC:\t", clf.__class__.__name__, "\t", np.mean(aucs)


def main():
    getAudioData()
    keys, depfeatures, labels = getDependentFeatures()
    twitter_features = getTwitterFeatures(keys)
    spotify_features = getSpotifyFeatures(keys)
    discogs_features = getDiscogsFeatures(keys)

    XIdxs = [0, 1, 2, 3]
    XNames = ["Dep", "Twitter", "Spotify", "Discogs"]
    Xs = [depfeatures, twitter_features, spotify_features,discogs_features]
    Y = labels

    # Test each feature set combination
    for L in range(0, len(XIdxs) + 1):
        for subset in itertools.combinations(XIdxs, L):
            if len(subset) == 0:
                continue

            out = ""
            for i in subset:
                out += XNames[i]
            print out

            XSubset = [Xs[i] for i in subset]
            X = np.hstack(tuple(XSubset))
        
            clf = linear_model.SGDClassifier(loss='log')
            test_classifier(clf, X, Y)

            clf = GaussianNB()
            test_classifier(clf, X, Y)

            clf = RandomForestClassifier(n_estimators=10, max_depth=10)
            test_classifier(clf, X, Y)

            print "\n"

if __name__ == '__main__':
    main()
