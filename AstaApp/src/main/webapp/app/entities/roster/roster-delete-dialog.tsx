import React, { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Button, Modal, ModalBody, ModalFooter, ModalHeader } from 'reactstrap';

import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';
import { deleteEntity, getEntity } from './roster.reducer';

export const RosterDeleteDialog = () => {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const { id } = useParams<'id'>();

  const [loadModal, setLoadModal] = useState(false);

  useEffect(() => {
    dispatch(getEntity(id));
    setLoadModal(true);
  }, []);

  const rosterEntity = useAppSelector(state => state.roster.entity);
  const updateSuccess = useAppSelector(state => state.roster.updateSuccess);

  const handleClose = () => {
    navigate('/roster');
  };

  useEffect(() => {
    if (updateSuccess && loadModal) {
      handleClose();
      setLoadModal(false);
    }
  }, [updateSuccess]);

  const confirmDelete = () => {
    dispatch(deleteEntity(rosterEntity.id));
  };

  return (
    <Modal isOpen toggle={handleClose}>
      <ModalHeader toggle={handleClose} data-cy="rosterDeleteDialogHeading">
        Conferma operazione di cancellazione
      </ModalHeader>
      <ModalBody id="astaAppApp.roster.delete.question">Sei sicuro di volere eliminare Roster {rosterEntity.id}?</ModalBody>
      <ModalFooter>
        <Button color="secondary" onClick={handleClose}>
          <FontAwesomeIcon icon="ban" />
          &nbsp; Annulla
        </Button>
        <Button id="jhi-confirm-delete-roster" data-cy="entityConfirmDeleteButton" color="danger" onClick={confirmDelete}>
          <FontAwesomeIcon icon="trash" />
          &nbsp; Elimina
        </Button>
      </ModalFooter>
    </Modal>
  );
};

export default RosterDeleteDialog;
