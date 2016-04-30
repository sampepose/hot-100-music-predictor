# conda install scikit-learn
import scipy
import numpy as np
from sklearn import linear_model
from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import StratifiedKFold

from getDependentFeatures import getDependentFeatures
from twitter.getFeatures import getTwitterFeatures

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
    print "Accuracy", clf.__class__.__name__, np.mean(accs)
    print "AUC", clf.__class__.__name__, np.mean(aucs)


def main():
    keys, depfeatures, labels = getDependentFeatures()
    twitter_features = getTwitterFeatures(keys)

    X = np.hstack((depfeatures, twitter_features)) # other features go here
    Y = labels

    print X[1:5, :]

    clf = linear_model.SGDClassifier(loss='log')
    test_classifier(clf, X, Y)

    clf = GaussianNB()
    test_classifier(clf, X, Y)

    clf = RandomForestClassifier(n_estimators=10, max_depth=10)
    test_classifier(clf, X, Y)

if __name__ == '__main__':
    main()
