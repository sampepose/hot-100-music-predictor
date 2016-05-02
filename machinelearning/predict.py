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
from spotify_data.machinelearning.getCounts import getDaysPlays

def classify(YPredProba, thresh):
    labels = []
    for yp in YPredProba:
        if yp > thresh:
            labels.append(1)
        else:
            labels.append(0)
    return labels

def accuracy(YTrue, YPred):
    correct, count = 0, 0
    for (yt, yp) in zip(YTrue, YPred):
        count += 1
        if yt == yp:
            correct += 1
    return float(correct) / count

def test_classifier(clf, X, Y):
    folds = StratifiedKFold(Y, 5)
    aucs = []
    accs = []

    #for x in np.arange(0, 1, 0.05):
    for train, test in folds:
            #aucs = []
            #accs = []

        clf.fit(X[train], Y[train])
        prediction = clf.predict(X[test])
        prediction_proba = clf.predict_proba(X[test])

        pred = classify(prediction_proba[:, 1], 0.6) # Value found via cross validation
        accs.append(accuracy(Y[test], pred))
        aucs.append(roc_auc_score(Y[test], prediction_proba[:, 1]))
    print "Acc:\t", clf.__class__.__name__, "\t", np.mean(accs)
    print "AUC:\t", clf.__class__.__name__, "\t", np.mean(aucs)


def main():
    getAudioData()
    keys, depfeatures, labels = getDependentFeatures()
    twitter_features = getTwitterFeatures(keys)
    spotify_features = getSpotifyFeatures(keys)
    discogs_features = getDiscogsFeatures(keys)
    
    days_plays_features = getDaysPlays(keys)

    XIdxs = [0, 1, 2, 3, 4]
    XNames = ["Dep", "Twitter", "Spotify", "Discogs", "DaysPlays"]
    Xs = [depfeatures, twitter_features, spotify_features, discogs_features, days_plays_features]
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
            
            clf = RandomForestClassifier(n_estimators=100, max_depth=100)
            test_classifier(clf, X, Y)

            print "\n"

if __name__ == '__main__':
    main()
